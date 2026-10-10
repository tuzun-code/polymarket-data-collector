import asyncio

from collector import run_all_collectors


async def main():

    await run_all_collectors(
        root="data"
    )


if __name__ == "__main__":
    asyncio.run(main())