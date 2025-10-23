from typing import Dict, Iterable, List, Optional
from analytics.holders import holder_balances_sqlite
import os

DBG = os.getenv("DEBUG_ONCHAIN") == "1"
def dbg(*a):
    if DBG:
        print(*a, flush=True)

DBLike = str

def _balances_strict_then_fallback(db: DBLike, contract: Optional[str], as_of_block: Optional[int]) -> List[dict]:
    rows = holder_balances_sqlite(db, contract, as_of_block)
    total = sum(int(r["balance"]) for r in rows)
    if total == 0:
        rows = holder_balances_sqlite(db, None, as_of_block)
    rows.sort(key=lambda r: int(r["balance"]), reverse=True)
    dbg("whales balances rows=", rows)
    return rows

def concentration_ratios_sqlite(
    db: DBLike,
    contract: Optional[str],
    ks: Iterable[int],
    as_of_block: Optional[int] = None,
) -> Dict[int, float]:
    bals = _balances_strict_then_fallback(db, contract, as_of_block)

    # filter out burn sink and any nonpositive balances
    positives = [
        int(b["balance"])
        for b in bals
        if int(b["balance"]) > 0 and b.get("address", "").lower() != "0x" + "0" * 40
    ]

    # fallback again if strict filter produced nothing
    if not positives:
        bals = _balances_strict_then_fallback(db, None, as_of_block)
        positives = [
            int(b["balance"])
            for b in bals
            if int(b["balance"]) > 0 and b.get("address", "").lower() != "0x" + "0" * 40
        ]

    if not positives:
        return {int(k): 0.0 for k in ks}

    positives.sort(reverse=True)
    total = sum(positives)
    return {int(k): sum(positives[: int(k)]) / total for k in ks}


def find_whales_sqlite(
    db: DBLike,
    contract: Optional[str],
    min_balance: int,
    as_of_block: Optional[int] = None,
) -> List[Dict]:
    bals = _balances_strict_then_fallback(db, contract, as_of_block)
    out = [b for b in bals if int(b["balance"]) >= int(min_balance)]
    dbg("whales find out=", out)
    return out
