from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from polymarket import AsyncPublicClient
from polymarket.streams import MarketSpec


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

WINDOW_SECONDS = 300
PRESTART_SECONDS = 10
POST_END_SECONDS = 3
WALLET_MATCH_INITIAL_DELAY_SECONDS = 5
WALLET_MATCH_RETRY_SECONDS = 5
WALLET_MATCH_MAX_ATTEMPTS = 5
WALLET_MATCH_TIME_TOLERANCE_SECONDS = 3.0
FLUSH_SECONDS = 2.0
BATCH_SIZE = 1000

ASSETS = {
    "BTC": "btc-updown-5m",
    "ETH": "eth-updown-5m",
    "XRP": "xrp-updown-5m",
    "DOGE": "doge-updown-5m",
}

GAMMA_API = "https://gamma-api.polymarket.com"


# ------------------------------------------------------------
# Market metadata
# ------------------------------------------------------------

@dataclass
class MarketMeta:
    asset: str
    event_id: str
    market_id: str
    event_slug: str
    market_slug: str
    question: str | None
    condition_id: str
    up_token_id: str
    down_token_id: str
    window_start: int
    window_end: int


def floor_5m(timestamp: int | float | None = None) -> int:
    if timestamp is None:
        timestamp = time.time()
    timestamp = int(timestamp)
    return timestamp - (timestamp % WINDOW_SECONDS)


def _outcome_for_token(market: MarketMeta, token_id: str | None) -> str | None:
    if token_id is None:
        return None
    if token_id == market.up_token_id:
        return "UP"
    if token_id == market.down_token_id:
        return "DOWN"
    return None


def _json_dump_model(value: Any) -> str:
    """Serialize SDK/Pydantic objects while preserving every available field."""
    if hasattr(value, "model_dump_json"):
        return value.model_dump_json()
    if hasattr(value, "model_dump"):
        return json.dumps(value.model_dump(), default=str, separators=(",", ":"))
    return json.dumps(value, default=str, separators=(",", ":"))


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ------------------------------------------------------------
# Gamma discovery
# ------------------------------------------------------------

async def discover_market(asset: str, window_start: int) -> MarketMeta | None:
    asset = asset.upper()
    if asset not in ASSETS:
        raise ValueError(f"Unsupported asset {asset!r}. Expected one of {list(ASSETS)}")

    event_slug = f"{ASSETS[asset]}-{window_start}"
    url = f"{GAMMA_API}/events/slug/{event_slug}"

    async with httpx.AsyncClient(timeout=15) as http:
        response = await http.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        event = response.json()

    markets = event.get("markets", [])
    if not markets:
        return None

    market = markets[0]
    outcomes = market.get("outcomes", [])
    token_ids = market.get("clobTokenIds", [])

    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)

    if len(outcomes) != len(token_ids):
        raise ValueError(f"Outcome/token mismatch for {event_slug}")

    token_map = {
        str(outcome).strip().lower(): str(token_id)
        for outcome, token_id in zip(outcomes, token_ids)
    }

    up_token = token_map.get("up")
    down_token = token_map.get("down")

    if not up_token or not down_token:
        raise ValueError(
            f"Could not find Up/Down token IDs for {event_slug}; outcomes={outcomes!r}"
        )

    condition_id = market.get("conditionId")
    if not condition_id:
        raise ValueError(f"No condition ID for {event_slug}")

    return MarketMeta(
        asset=asset,
        event_id=str(event.get("id")),
        market_id=str(market.get("id")),
        event_slug=event_slug,
        market_slug=str(market.get("slug") or event_slug),
        question=market.get("question"),
        condition_id=str(condition_id),
        up_token_id=up_token,
        down_token_id=down_token,
        window_start=window_start,
        window_end=window_start + WINDOW_SECONDS,
    )


# ------------------------------------------------------------
# Tick Parquet writer
# ------------------------------------------------------------

class TickWriter:
    """
    Stores raw websocket data while also flattening the most useful fields.

    Important: price_change events can contain several changed price levels.
    They are written as one row per individual change, while event_json keeps
    the complete original websocket event for lossless reprocessing later.
    """

    schema = pa.schema(
        [
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("event_timestamp", pa.timestamp("us", tz="UTC")),
            ("sequence", pa.int64()),
            ("sub_index", pa.int32()),
            ("source", pa.string()),
            ("asset", pa.string()),
            ("event_slug", pa.string()),
            ("market_id", pa.string()),
            ("condition_id", pa.string()),
            ("window_start", pa.int64()),
            ("window_end", pa.int64()),
            ("event_type", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("side", pa.string()),
            ("price", pa.string()),
            ("size", pa.string()),
            ("best_bid", pa.string()),
            ("best_ask", pa.string()),
            ("transaction_hash", pa.string()),
            ("event_json", pa.string()),
        ]
    )

    def __init__(
        self,
        directory: Path,
        market: MarketMeta,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self.directory = Path(directory)
        self.market = market
        self.directory.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.buffer: list[dict[str, Any]] = []
        self.sequence = 0

    def _append_row(
        self,
        *,
        event_timestamp: datetime | None,
        source: str,
        event_type: str,
        event_json: str,
        token_id: str | None = None,
        side: Any = None,
        price: Any = None,
        size: Any = None,
        best_bid: Any = None,
        best_ask: Any = None,
        transaction_hash: Any = None,
        sub_index: int = 0,
    ) -> None:
        token_id = str(token_id) if token_id is not None else None
        if hasattr(side, "value"):
            side = side.value

        self.buffer.append(
            {
                "received_at": datetime.now(timezone.utc),
                "event_timestamp": _ensure_utc(event_timestamp),
                "sequence": self.sequence,
                "sub_index": sub_index,
                "source": source,
                "asset": self.market.asset,
                "event_slug": self.market.event_slug,
                "market_id": self.market.market_id,
                "condition_id": self.market.condition_id,
                "window_start": self.market.window_start,
                "window_end": self.market.window_end,
                "event_type": event_type,
                "token_id": token_id,
                "outcome": _outcome_for_token(self.market, token_id),
                "side": str(side) if side is not None else None,
                "price": str(price) if price is not None else None,
                "size": str(size) if size is not None else None,
                "best_bid": str(best_bid) if best_bid is not None else None,
                "best_ask": str(best_ask) if best_ask is not None else None,
                "transaction_hash": (
                    str(transaction_hash) if transaction_hash is not None else None
                ),
                "event_json": event_json,
            }
        )

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def add_rest_snapshot(self, book: Any, outcome: str | None = None) -> None:
        """Persist an authoritative REST order-book snapshot immediately."""
        token_id = str(getattr(book, "asset_id", getattr(book, "token_id", "")))
        event_json = _json_dump_model(book)
        timestamp = getattr(book, "timestamp", None)

        self._append_row(
            event_timestamp=timestamp,
            source="rest",
            event_type="rest_book_snapshot",
            event_json=event_json,
            token_id=token_id,
        )

        # A REST snapshot is important recovery state; write it immediately.
        self.flush()

    def add(self, event: Any) -> None:
        payload = event.payload
        event_type = str(event.type)
        event_json = _json_dump_model(event)
        timestamp = getattr(payload, "timestamp", None)

        # price_change is special: one websocket event can contain several
        # token/price-level changes. Store one row per change.
        if event_type == "price_change":
            changes = getattr(payload, "price_changes", ())
            if not changes:
                self._append_row(
                    event_timestamp=timestamp,
                    source="websocket",
                    event_type=event_type,
                    event_json=event_json,
                )
            else:
                for index, change in enumerate(changes):
                    token_id = getattr(
                        change, "asset_id", getattr(change, "token_id", None)
                    )
                    self._append_row(
                        event_timestamp=timestamp,
                        source="websocket",
                        event_type=event_type,
                        event_json=event_json,
                        token_id=token_id,
                        side=getattr(change, "side", None),
                        price=getattr(change, "price", None),
                        size=getattr(change, "size", None),
                        best_bid=getattr(change, "best_bid", None),
                        best_ask=getattr(change, "best_ask", None),
                        sub_index=index,
                    )

            self.sequence += 1
            return

        token_id = getattr(payload, "asset_id", getattr(payload, "token_id", None))

        self._append_row(
            event_timestamp=timestamp,
            source="websocket",
            event_type=event_type,
            event_json=event_json,
            token_id=token_id,
            side=getattr(payload, "side", None),
            price=getattr(payload, "price", None),
            size=getattr(payload, "size", None),
            best_bid=getattr(payload, "best_bid", None),
            best_ask=getattr(payload, "best_ask", None),
            transaction_hash=getattr(payload, "transaction_hash", None),
        )
        self.sequence += 1

    def flush(self) -> None:
        if not self.buffer:
            return

        rows = self.buffer
        self.buffer = []

        filename = self.directory / (
            f"part-{time.time_ns()}-{uuid.uuid4().hex[:8]}.parquet"
        )

        table = pa.Table.from_pylist(rows, schema=self.schema)
        pq.write_table(table, filename, compression="zstd")

        print(
            f"[{self.market.asset}] wrote {len(rows):,} tick rows -> {filename.name}"
        )

    def close(self) -> None:
        self.flush()


async def periodic_flush(writer: TickWriter, every_seconds: float = FLUSH_SECONDS) -> None:
    """Flush quiet markets too; they may never reach batch_size."""
    while True:
        await asyncio.sleep(every_seconds)
        writer.flush()


# ------------------------------------------------------------
# Wallet / trade collection + websocket matching
# ------------------------------------------------------------

def _decimal_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError, TypeError):
        return str(left) == str(right)


def _time_distance_seconds(left: datetime | None, right: datetime | None) -> float | None:
    left = _ensure_utc(left)
    right = _ensure_utc(right)
    if left is None or right is None:
        return None
    return abs((left - right).total_seconds())


def _trade_row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Stable-enough key used only to de-duplicate repeated API polling results."""
    return (
        row.get("transaction_hash"),
        row.get("proxy_wallet"),
        row.get("token_id"),
        row.get("side"),
        row.get("price"),
        row.get("size"),
        row.get("timestamp"),
    )


async def fetch_wallet_trades(market: MarketMeta) -> list[dict[str, Any]]:
    """Fetch public wallet-attributed trade rows for this market."""
    rows: list[dict[str, Any]] = []

    async with AsyncPublicClient() as client:
        paginator = client.list_trades(
            condition_id=market.condition_id,
            taker_only=False,
            full_history=True,
            page_size=100,
        )

        async for trade in paginator.iter_items():
            tx_hash = (
                str(trade.transaction_hash)
                if trade.transaction_hash is not None
                else None
            )
            token_id = str(trade.asset_id)
            side = trade.side.value if hasattr(trade.side, "value") else trade.side

            rows.append(
                {
                    "asset": market.asset,
                    "event_slug": market.event_slug,
                    "market_id": market.market_id,
                    "condition_id": market.condition_id,
                    "window_start": market.window_start,
                    "window_end": market.window_end,
                    "timestamp": _ensure_utc(trade.timestamp),
                    "proxy_wallet": str(trade.wallet),
                    "token_id": token_id,
                    "outcome": _outcome_for_token(market, token_id)
                    or getattr(trade, "outcome", None),
                    "side": str(side),
                    "price": str(trade.price),
                    "size": str(trade.size),
                    "transaction_hash": tx_hash,
                    "name": getattr(trade, "name", None),
                    "pseudonym": getattr(trade, "pseudonym", None),
                }
            )

    # Polling can return the same rows repeatedly. Keep one copy.
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        unique[_trade_row_key(row)] = row
    return list(unique.values())


def build_wallet_matches(
    ws_trade_events: list[dict[str, Any]],
    wallet_trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Match websocket last_trade_price events to public wallet trade records.

    Matching policy:
      1. transaction_hash + token/price/size when available -> very_high
      2. transaction_hash only                              -> high
      3. token/price/size + nearby timestamp                -> medium

    A blockchain transaction can contain multiple fills, so this function keeps
    *all* plausible wallet candidates instead of forcing a false one-to-one join.
    """
    by_hash: dict[str, list[dict[str, Any]]] = {}
    for trade in wallet_trades:
        tx_hash = trade.get("transaction_hash")
        if tx_hash:
            by_hash.setdefault(str(tx_hash).lower(), []).append(trade)

    output: list[dict[str, Any]] = []

    for ws in ws_trade_events:
        tx_hash = ws.get("transaction_hash")
        token_id = ws.get("token_id")
        ws_price = ws.get("price")
        ws_size = ws.get("size")
        ws_time = ws.get("event_timestamp")

        candidates: list[dict[str, Any]] = []
        match_method = "unmatched"
        confidence = "none"

        if tx_hash:
            hash_candidates = by_hash.get(str(tx_hash).lower(), [])

            if hash_candidates:
                exact_candidates = [
                    trade
                    for trade in hash_candidates
                    if (
                        (token_id is None or trade.get("token_id") == token_id)
                        and (ws_price is None or _decimal_equal(trade.get("price"), ws_price))
                        and (ws_size is None or _decimal_equal(trade.get("size"), ws_size))
                    )
                ]

                if exact_candidates:
                    candidates = exact_candidates
                    match_method = "transaction_hash+token+price+size"
                    confidence = "very_high"
                else:
                    candidates = hash_candidates
                    match_method = "transaction_hash"
                    confidence = "high"

        # Fallback only when no transaction-hash match was found. Requiring the
        # same token, price and size plus a tight time window keeps this fairly
        # conservative.
        if not candidates:
            fallback: list[dict[str, Any]] = []
            for trade in wallet_trades:
                if token_id is not None and trade.get("token_id") != token_id:
                    continue
                if not _decimal_equal(trade.get("price"), ws_price):
                    continue
                if not _decimal_equal(trade.get("size"), ws_size):
                    continue

                distance = _time_distance_seconds(ws_time, trade.get("timestamp"))
                if distance is None or distance > WALLET_MATCH_TIME_TOLERANCE_SECONDS:
                    continue

                fallback.append(trade)

            if fallback:
                candidates = fallback
                match_method = "token+price+size+time"
                confidence = "medium"

        if not candidates:
            output.append(
                {
                    "matched": False,
                    "match_method": "unmatched",
                    "match_confidence": "none",
                    "candidate_count": 0,
                    "ws_sequence": ws.get("sequence"),
                    "ws_received_at": ws.get("received_at"),
                    "ws_timestamp": ws_time,
                    "ws_token_id": token_id,
                    "ws_outcome": ws.get("outcome"),
                    "ws_side": ws.get("side"),
                    "ws_price": ws_price,
                    "ws_size": ws_size,
                    "transaction_hash": tx_hash,
                    "proxy_wallet": None,
                    "wallet_trade_timestamp": None,
                    "wallet_token_id": None,
                    "wallet_outcome": None,
                    "wallet_side": None,
                    "wallet_price": None,
                    "wallet_size": None,
                    "wallet_name": None,
                    "wallet_pseudonym": None,
                    "time_distance_seconds": None,
                }
            )
            continue

        for candidate in candidates:
            output.append(
                {
                    "matched": True,
                    "match_method": match_method,
                    "match_confidence": confidence,
                    "candidate_count": len(candidates),
                    "ws_sequence": ws.get("sequence"),
                    "ws_received_at": ws.get("received_at"),
                    "ws_timestamp": ws_time,
                    "ws_token_id": token_id,
                    "ws_outcome": ws.get("outcome"),
                    "ws_side": ws.get("side"),
                    "ws_price": ws_price,
                    "ws_size": ws_size,
                    "transaction_hash": tx_hash or candidate.get("transaction_hash"),
                    "proxy_wallet": candidate.get("proxy_wallet"),
                    "wallet_trade_timestamp": candidate.get("timestamp"),
                    "wallet_token_id": candidate.get("token_id"),
                    "wallet_outcome": candidate.get("outcome"),
                    "wallet_side": candidate.get("side"),
                    "wallet_price": candidate.get("price"),
                    "wallet_size": candidate.get("size"),
                    "wallet_name": candidate.get("name"),
                    "wallet_pseudonym": candidate.get("pseudonym"),
                    "time_distance_seconds": _time_distance_seconds(
                        ws_time, candidate.get("timestamp")
                    ),
                }
            )

    return output


def _write_parquet_rows(rows: list[dict[str, Any]], filename: Path) -> None:
    filename.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, filename, compression="zstd")


async def save_wallet_trades_and_matches(
    market: MarketMeta,
    directory: Path,
    ws_trade_events: list[dict[str, Any]],
) -> None:
    """
    Wait briefly for Data API ingestion, then retry a few times while matching
    websocket trades to wallet-attributed public trade rows.
    """
    directory.mkdir(parents=True, exist_ok=True)

    if WALLET_MATCH_INITIAL_DELAY_SECONDS > 0:
        await asyncio.sleep(WALLET_MATCH_INITIAL_DELAY_SECONDS)

    wallet_trades: list[dict[str, Any]] = []
    matches: list[dict[str, Any]] = []

    for attempt in range(1, WALLET_MATCH_MAX_ATTEMPTS + 1):
        try:
            wallet_trades = await fetch_wallet_trades(market)
            matches = build_wallet_matches(ws_trade_events, wallet_trades)
        except Exception as exc:
            print(
                f"[{market.asset}] wallet match attempt {attempt}/"
                f"{WALLET_MATCH_MAX_ATTEMPTS} failed: {exc}"
            )
            if attempt < WALLET_MATCH_MAX_ATTEMPTS:
                await asyncio.sleep(WALLET_MATCH_RETRY_SECONDS)
            continue

        matched_sequences = {
            row["ws_sequence"]
            for row in matches
            if row.get("matched") and row.get("ws_sequence") is not None
        }
        total_ws = len(ws_trade_events)
        matched_ws = len(matched_sequences)

        print(
            f"[{market.asset}] wallet match attempt {attempt}/"
            f"{WALLET_MATCH_MAX_ATTEMPTS}: "
            f"{matched_ws}/{total_ws} websocket trades matched; "
            f"{len(wallet_trades)} public trade rows available"
        )

        # Nothing to match, or every websocket trade now has at least one
        # plausible wallet candidate.
        if total_ws == 0 or matched_ws >= total_ws:
            break

        if attempt < WALLET_MATCH_MAX_ATTEMPTS:
            await asyncio.sleep(WALLET_MATCH_RETRY_SECONDS)

    # If every API attempt failed, still persist one explicit unmatched row per
    # websocket trade instead of silently omitting the match file.
    if ws_trade_events and not matches:
        matches = build_wallet_matches(ws_trade_events, wallet_trades)

    if wallet_trades:
        _write_parquet_rows(wallet_trades, directory / "wallet_trades.parquet")
        print(
            f"[{market.asset}] wrote {len(wallet_trades):,} wallet-trade rows"
        )
    else:
        print(f"[{market.asset}] no wallet trades found after retries")

    # Always emit a wallet_matches dataset when there is wallet-attributed
    # Data API activity. If the websocket did not emit last_trade_price events,
    # keep the direct wallet <-> transaction links from the Data API and mark
    # them explicitly as data_api_only rather than pretending they were WS matches.
    if not ws_trade_events and wallet_trades:
        matches = []
        for trade in wallet_trades:
            matches.append(
                {
                    "matched": False,
                    "match_method": "data_api_only",
                    "match_confidence": "api_direct_wallet_transaction",
                    "candidate_count": 0,
                    "ws_sequence": None,
                    "ws_received_at": None,
                    "ws_timestamp": None,
                    "ws_token_id": None,
                    "ws_outcome": None,
                    "ws_side": None,
                    "ws_price": None,
                    "ws_size": None,
                    "transaction_hash": trade.get("transaction_hash"),
                    "proxy_wallet": trade.get("proxy_wallet"),
                    "wallet_trade_timestamp": trade.get("timestamp"),
                    "wallet_token_id": trade.get("token_id"),
                    "wallet_outcome": trade.get("outcome"),
                    "wallet_side": trade.get("side"),
                    "wallet_price": trade.get("price"),
                    "wallet_size": trade.get("size"),
                    "wallet_name": trade.get("name"),
                    "wallet_pseudonym": trade.get("pseudonym"),
                    "time_distance_seconds": None,
                }
            )

        print(
            f"[{market.asset}] no websocket last_trade_price events captured; "
            f"preserving {len(matches):,} direct Data API wallet->transaction rows"
        )

    if matches:
        # This file now always contains the best wallet/transaction enrichment
        # available: true WS matches when possible, otherwise direct Data API
        # wallet->transaction records marked with match_method=data_api_only.
        _write_parquet_rows(matches, directory / "wallet_matches.parquet")
        matched_rows = sum(1 for row in matches if row.get("matched"))
        api_only_rows = sum(
            1 for row in matches if row.get("match_method") == "data_api_only"
        )
        print(
            f"[{market.asset}] wrote {len(matches):,} wallet-match rows "
            f"({matched_rows:,} websocket-matched, {api_only_rows:,} data-api-only)"
        )
    elif not ws_trade_events:
        print(
            f"[{market.asset}] no websocket trade events and no public wallet trades; "
            "no wallet-match rows to write"
        )


# ------------------------------------------------------------
# One 5-minute market window
# ------------------------------------------------------------

async def collect_market_window(
    asset: str,
    window_start: int,
    root: str | Path = "data",
) -> None:
    asset = asset.upper()
    root = Path(root)

    start_collecting = window_start - PRESTART_SECONDS
    delay = start_collecting - time.time()
    if delay > 0:
        await asyncio.sleep(delay)

    market: MarketMeta | None = None

    # Upcoming recurring markets can appear shortly before the window.
    while market is None:
        try:
            market = await discover_market(asset, window_start)
        except Exception as exc:
            print(f"[{asset}] discovery error: {exc}")

        if market is None:
            if time.time() > window_start + 60:
                print(f"[{asset}] could not discover market for {window_start}")
                return
            await asyncio.sleep(1)

    print(f"\n[{asset}] {market.event_slug}")
    print(f"[{asset}] UP   = {market.up_token_id}")
    print(f"[{asset}] DOWN = {market.down_token_id}")

    day = datetime.fromtimestamp(window_start, tz=timezone.utc).strftime("%Y-%m-%d")
    market_dir = root / asset / day / market.event_slug
    tick_dir = market_dir / "ticks"
    trade_dir = market_dir / "trades"
    market_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "asset": market.asset,
        "event_id": market.event_id,
        "market_id": market.market_id,
        "event_slug": market.event_slug,
        "market_slug": market.market_slug,
        "question": market.question,
        "condition_id": market.condition_id,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
        "window_start": market.window_start,
        "window_end": market.window_end,
    }
    with (market_dir / "market.json").open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    writer = TickWriter(directory=tick_dir, market=market)
    ws_transactions: Counter[str] = Counter()
    ws_trade_events: list[dict[str, Any]] = []
    event_counts: Counter[str] = Counter()
    stop_at = market.window_end + POST_END_SECONDS

    try:
        async with AsyncPublicClient() as client:
            # Subscribe first, so websocket events can queue while the initial
            # REST snapshots are fetched. This minimizes the startup gap.
            async with await client.subscribe(
                MarketSpec(
                    asset_ids=[market.up_token_id, market.down_token_id],
                    custom_feature_enabled=True,
                )
            ) as stream:
                books = await client.get_order_books(
                    asset_ids=[market.up_token_id, market.down_token_id]
                )

                for book in books:
                    token_id = str(book.asset_id)
                    writer.add_rest_snapshot(
                        book,
                        outcome=_outcome_for_token(market, token_id),
                    )

                async def consume() -> None:
                    async for event in stream:
                        event_type = str(event.type)
                        event_counts[event_type] += 1

                        # Capture the row sequence before writer.add() advances it.
                        ws_sequence = writer.sequence
                        received_at = datetime.now(timezone.utc)
                        writer.add(event)

                        if event_type == "last_trade_price":
                            payload = event.payload
                            tx_hash = getattr(payload, "transaction_hash", None)
                            token_id = getattr(
                                payload, "asset_id", getattr(payload, "token_id", None)
                            )
                            token_id = str(token_id) if token_id is not None else None
                            side = getattr(payload, "side", None)
                            if hasattr(side, "value"):
                                side = side.value

                            if tx_hash:
                                ws_transactions[str(tx_hash)] += 1

                            ws_trade_events.append(
                                {
                                    "sequence": ws_sequence,
                                    "received_at": received_at,
                                    "event_timestamp": _ensure_utc(
                                        getattr(payload, "timestamp", None)
                                    ),
                                    "token_id": token_id,
                                    "outcome": _outcome_for_token(market, token_id),
                                    "side": str(side) if side is not None else None,
                                    "price": (
                                        str(getattr(payload, "price", None))
                                        if getattr(payload, "price", None) is not None
                                        else None
                                    ),
                                    "size": (
                                        str(getattr(payload, "size", None))
                                        if getattr(payload, "size", None) is not None
                                        else None
                                    ),
                                    "transaction_hash": (
                                        str(tx_hash) if tx_hash is not None else None
                                    ),
                                }
                            )

                consumer = asyncio.create_task(consume())
                flusher = asyncio.create_task(periodic_flush(writer))

                try:
                    remaining = stop_at - time.time()
                    if remaining > 0:
                        await asyncio.sleep(remaining)
                finally:
                    consumer.cancel()
                    flusher.cancel()
                    await asyncio.gather(
                        consumer,
                        flusher,
                        return_exceptions=True,
                    )
    finally:
        writer.close()

    print(f"[{asset}] websocket events: {dict(event_counts)}")
    print(f"[{asset}] captured {len(ws_trade_events)} websocket trade events")

    try:
        await save_wallet_trades_and_matches(
            market=market,
            directory=trade_dir,
            ws_trade_events=ws_trade_events,
        )
    except Exception as exc:
        # Tick collection is the primary dataset; wallet enrichment should not
        # kill the scheduler if the Data API is temporarily unavailable.
        print(f"[{asset}] wallet matching failed: {exc}")


# ------------------------------------------------------------
# Continuous schedulers
# ------------------------------------------------------------

async def asset_scheduler(asset: str, root: str | Path = "data") -> None:
    asset = asset.upper()
    tasks: dict[int, asyncio.Task[None]] = {}

    while True:
        current = floor_5m()

        # Arm current and next 5-minute windows. If started mid-window,
        # current begins collecting immediately for the remaining time.
        for window_start in (current, current + WINDOW_SECONDS):
            if window_start not in tasks:
                tasks[window_start] = asyncio.create_task(
                    collect_market_window(asset, window_start, root)
                )

        finished = [
            window_start
            for window_start, task in tasks.items()
            if task.done() and window_start < current
        ]

        for window_start in finished:
            task = tasks.pop(window_start)
            try:
                task.result()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{asset}] window task failed ({window_start}): {exc}")

        await asyncio.sleep(1)


async def run_all_collectors(
    root: str | Path = "data",
    assets: tuple[str, ...] = ("BTC", "ETH", "XRP", "DOGE"),
) -> None:
    await asyncio.gather(
        *(asset_scheduler(asset, root=root) for asset in assets)
    )
