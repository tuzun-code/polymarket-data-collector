import json
import requests


GAMMA_URL = "https://gamma-api.polymarket.com"


def search_markets(query: str, limit: int = 10):
    """
    Search Polymarket by human-readable text.

    Returns a list of individual markets with:
        market_id
        event_title
        question
        slug
        condition_id
        yes_token_id
        no_token_id
    """

    response = requests.get(
        f"{GAMMA_URL}/public-search",
        params={
            "q": query,
            "limit_per_type": limit,
            "keep_closed_markets": 0,
        },
        timeout=20,
    )

    response.raise_for_status()
    data = response.json()

    results = []

    for event in data.get("events", []):

        for market in event.get("markets", []):

            # Gamma often returns these fields as JSON strings.
            outcomes = market.get("outcomes", [])
            token_ids = market.get("clobTokenIds", [])

            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)

            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)

            # Build outcome -> token mapping instead of assuming
            # positions are always YES, NO.
            tokens = {
                outcome: token_id
                for outcome, token_id in zip(
                    outcomes,
                    token_ids,
                )
            }

            results.append({
                "market_id": market.get("id"),
                "event_title": event.get("title"),
                "question": market.get("question"),
                "slug": market.get("slug"),
                "condition_id": market.get("conditionId"),

                "yes_token_id": tokens.get("Yes"),
                "no_token_id": tokens.get("No"),

                "active": market.get("active"),
                "closed": market.get("closed"),
                "accepting_orders": market.get(
                    "acceptingOrders"
                ),
            })

    return results

def print_market_search(query: str, limit: int = 10):

    markets = search_markets(
        query,
        limit=limit,
    )

    if not markets:
        print("No markets found.")
        return []

    for i, market in enumerate(markets):

        print()
        print("=" * 70)
        print(f"[{i}] {market['question']}")
        print(f"Event: {market['event_title']}")
        print(f"Market ID: {market['market_id']}")
        print(f"Condition ID: {market['condition_id']}")
        print(f"Slug: {market['slug']}")
        print()
        print(f"YES ID: {market['yes_token_id']}")
        print(f"NO  ID: {market['no_token_id']}")
        print(
            f"Accepting orders: "
            f"{market['accepting_orders']}"
        )

    return markets