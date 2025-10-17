import argparse
from etl.pipeline import run_etl

def main():
    p = argparse.ArgumentParser(description="Run onchain ETL pipeline")
    p.add_argument("--block", type=int, required=True, help="Block number to ingest")
    p.add_argument("--backend", choices=["sqlite", "postgres"], default="sqlite",
                   help="Storage backend")
    p.add_argument("--sqlite-path", dest="sqlite_path", default=None,
                   help="Path to SQLite DB file (e.g., data/dev.db)")
    p.add_argument("--pg-dsn", dest="pg_dsn", default=None,
                   help="Postgres DSN (e.g., postgresql://user:pass@host:5432/db)")
    args = p.parse_args()

    total = run_etl(
        args.block,
        backend=args.backend,
        sqlite_path=args.sqlite_path,
        pg_dsn=args.pg_dsn,
    )
    print(f"Ingested {total} records from block {args.block}")

if __name__ == "__main__":
    main()
