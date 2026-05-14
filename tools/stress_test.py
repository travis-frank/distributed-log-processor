#!/usr/bin/env python3

import argparse
import asyncio
import json
import random
import time
from datetime import datetime, timezone


ACCOUNT_IDS = [f"acc_{i:04d}" for i in range(1000)]
ENTRY_TYPES = ["deposit", "withdrawal", "transfer", "fee"]


def make_entry() -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": random.choice(ACCOUNT_IDS),
        "amount": round(random.uniform(10.0, 50000.0), 2),
        "type": random.choice(ENTRY_TYPES),
        "currency": "USD",
    }


async def run_client(host: str, port: int, messages: int) -> tuple[int, int]:
    sent = 0
    errors = 0
    writer = None

    try:
        _, writer = await asyncio.open_connection(host, port)
        for _ in range(messages):
            line = json.dumps(make_entry(), separators=(",", ":")) + "\n"
            try:
                writer.write(line.encode("utf-8"))
                await writer.drain()
                sent += 1
            except Exception:
                errors += 1
                break
    except Exception:
        errors += messages
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    return sent, errors


async def main() -> None:
    parser = argparse.ArgumentParser(description="Async TCP stress tester")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--messages", type=int, default=1000)
    args = parser.parse_args()

    start = time.perf_counter()
    tasks = [run_client(args.host, args.port, args.messages) for _ in range(args.clients)]
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    total_sent = sum(sent for sent, _ in results)
    total_errors = sum(err for _, err in results)
    attempted = args.clients * args.messages
    throughput = total_sent / elapsed if elapsed > 0 else 0.0
    error_rate = (total_errors / attempted * 100.0) if attempted > 0 else 0.0

    print(f"Total messages sent: {total_sent}")
    print(f"Total errors: {total_errors}")
    print(f"Elapsed time (s): {elapsed:.3f}")
    print(f"Throughput (messages/sec): {throughput:.2f}")
    print(f"Error rate (%): {error_rate:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
