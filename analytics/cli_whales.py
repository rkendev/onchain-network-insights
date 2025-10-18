# analytics/cli_whales.py (optional)
import argparse
from analytics.whales import find_whales_sqlite, concentration_ratios_sqlite

def main():
    p = argparse.ArgumentParser(description="Whale analytics (SQLite)")
    p.add_argument("--db", required=True, help="Path to SQLite DB")
    p.add_argument("--contract", required=True, help="ERC-20 contract")
    p.add_argument("--min-balance", type=int, default=1_000_000)
    p.add_argument("--as-of", type=int, default=None)
    p.add_argument("--show-cr", action="store_true", help="Show concentration ratios")
    args = p.parse_args()

    whales = find_whales_sqlite(args.db, args.contract, args.min_balance, args.as_of)
    print(f"Whales (balance >= {args.min_balance}):")
    for i, w in enumerate(whales, 1):
        print(f"{i:02d}. {w['address']}  {w['balance']}")

    if args.show_cr:
        cr = concentration_ratios_sqlite(args.db, args.contract, as_of_block=args.as_of)
        print("\nConcentration ratios:")
        for k in sorted(cr):
            print(f"CR{k:>3}: {cr[k]:.4f}")

if __name__ == "__main__":
    main()
