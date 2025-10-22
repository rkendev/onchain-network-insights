from __future__ import annotations
from typing import List, Tuple, Optional, Dict
from analytics.holders import holder_balances_sqlite

def find_whales_sqlite(db_path: str, contract: str, min_balance: int, as_of_block: Optional[int] = None) -> List[Dict[str, int]]:
    bals = holder_balances_sqlite(db_path, contract, as_of_block)
    whales = [x for x in bals if int(x["balance"]) >= int(min_balance)]
    return whales  # already sorted desc by balance

def concentration_ratios_sqlite(db_path: str, contract: str, ks=(1,2,3,5,10,50,100), as_of_block: Optional[int] = None) -> List[float]:
    bals = holder_balances_sqlite(db_path, contract, as_of_block)
    vals = [int(x["balance"]) for x in bals if int(x["balance"]) > 0]
    total = sum(vals) or 1
    vals.sort(reverse=True)
    out = []
    for k in ks:
        out.append(sum(vals[:k]) / total if vals else 0.0)
    return out
