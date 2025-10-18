import argparse
from analytics.token_holders import top_holders_sqlite

def main():
    p = argparse.ArgumentParser(description="Token holders analytics (SQLite)")
    p.add_argument("--db", required=True, help="Path to SQLite DB")
    p.add_argument("--contract", required=True, help="ERC-20 contract address")
    p.add_argument("--top", type=int, default=10, help="Top N holders")
    p.add_argument("--as-of", type=int, default=None, help="As-of block number")
    args = p.parse_args()

    rows = top_holders_sqlite(args.db, args.contract, n=args.top, as_of_block=args.as_of)
    for i, r in enumerate(rows, 1):
        print(f"{i:02d}. {r['address']}  {r['balance']}")

if __name__ == "__main__":
    main()
