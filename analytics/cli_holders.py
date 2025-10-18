# analytics/cli_holders.py
import argparse
from analytics.holders import (
    holder_balances_sqlite, holder_deltas_sqlite,
    top_gainers_sqlite, top_spenders_sqlite, distribution_metrics_sqlite
)

def main():
    p = argparse.ArgumentParser(description="Token holder analytics (SQLite)")
    p.add_argument("--db", required=True, help="Path to SQLite DB")
    p.add_argument("--contract", required=True, help="ERC-20 contract")
    p.add_argument("--as-of", type=int, default=None, help="As-of block for balances")
    p.add_argument("--window-start", type=int, default=None, help="Start block (exclusive)")
    p.add_argument("--window-end", type=int, default=None, help="End block (inclusive)")
    p.add_argument("--top", type=int, default=5, help="Top N for gainers/spenders")
    args = p.parse_args()

    bals = holder_balances_sqlite(args.db, args.contract, args.as_of)
    print(f"Balances (as-of {args.as_of}): {len(bals)} holders")
    for i, r in enumerate(bals[:args.top], 1):
        print(f"{i:02d}. {r['address']}  {r['balance']}")

    if args.window_start is not None and args.window_end is not None:
        print(f"\nWindow: ({args.window_start}, {args.window_end}]")
        print("Top gainers:")
        for r in top_gainers_sqlite(args.db, args.contract, args.top, args.window_start, args.window_end):
            print(f" + {r['address']}  {r['delta']}")
        print("Top spenders:")
        for r in top_spenders_sqlite(args.db, args.contract, args.top, args.window_start, args.window_end):
            print(f" - {r['address']}  {r['delta']}")

    m = distribution_metrics_sqlite(args.db, args.contract, args.as_of)
    print(f"\nDistribution metrics: Gini={m['gini']:.4f}  HHI={m['hhi']:.4f}")

if __name__ == "__main__":
    main()
