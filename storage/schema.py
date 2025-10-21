# storage/schema.py
CREATE_TABLE_BLOCKS = """
CREATE TABLE IF NOT EXISTS blocks (
    block_number BIGINT PRIMARY KEY,
    block_hash TEXT,
    timestamp BIGINT
);
"""

CREATE_TABLE_TXS = """
CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT PRIMARY KEY,
    from_address TEXT,
    to_address TEXT,
    value TEXT
);
"""

CREATE_TABLE_LOGS = """
CREATE TABLE IF NOT EXISTS logs (
    tx_hash TEXT,
    address TEXT,
    data TEXT,
    PRIMARY KEY (tx_hash, address)
);
"""

CREATE_TABLE_TRANSFERS = """
CREATE TABLE IF NOT EXISTS transfers (
    tx_hash TEXT,
    contract TEXT,
    sender TEXT,
    recipient TEXT,
    value INTEGER,
    block_number BIGINT,
    PRIMARY KEY (tx_hash, contract, sender, recipient, block_number)
);
"""


BALANCES_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS balances_view AS
SELECT contract,
       address,
       SUM(amt) AS balance
FROM (
    SELECT contract, "to"   AS address, value    AS amt FROM transfers
    UNION ALL
    SELECT contract, "from" AS address, -value   AS amt FROM transfers
)
GROUP BY contract, address;
"""

# --- View helpers for analytics (idempotent) -------------------------------

def ensure_analytics_views(con):
    """
    Create lightweight views the dashboard expects.
    Safe to call repeatedly.
    """
    cur = con.cursor()

    # View: transfers_enriched
    #   Flattens each transfer into +/- deltas for from/to addresses.
    cur.execute("""
    CREATE VIEW IF NOT EXISTS transfers_enriched AS
    SELECT
        contract,
        "to"      AS address,
        value     AS delta,
        blockNumber
    FROM transfers
    UNION ALL
    SELECT
        contract,
        "from"    AS address,
        -value    AS delta,
        blockNumber
    FROM transfers;
    """)

    # View: mint_burn (optional metadata helper; zero-address mints/burns)
    cur.execute("""
    CREATE VIEW IF NOT EXISTS mint_burn AS
    WITH zero AS (
        SELECT lower('0x0000000000000000000000000000000000000000') AS z
    )
    SELECT
        t.contract,
        SUM(CASE WHEN lower(t."from") = z THEN t.value ELSE 0 END) AS total_minted,
        SUM(CASE WHEN lower(t."to")   = z THEN t.value ELSE 0 END) AS total_burned
    FROM transfers t, zero
    GROUP BY t.contract;
    """)

    con.commit()
