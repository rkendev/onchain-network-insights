import argparse
from etl.pipeline import run_etl

def main():
    p = argparse.ArgumentParser(description="Run onchain ETL pipeline")
    p.add_argument("--block", type=int, required=True)
    p.add_argument("--backend", choices=["sqlite","postgres"], default="sqlite")
    args = p.parse_args()
    total = run_etl(args.block, backend=args.backend)
    print(f"Ingested {total} records from block {args.block}")

if __name__ == "__main__":
    main()
