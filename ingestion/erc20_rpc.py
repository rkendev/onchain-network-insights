# ingestion/erc20_rpc.py  (replace the file top-to-bottom only where different)
from __future__ import annotations

import os
import time
from typing import Optional, List
import requests
import re

_raw = os.environ.get("RPC_URL_OVERRIDE", "").strip()
RPC_URLS: List[str] = [u.strip() for u in _raw.split(",") if u.strip()]

class RpcError(RuntimeError):
    pass

_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")

def normalize_contract(addr: str) -> str:
    """
    Returns lowercased 0x-prefixed 40-hex address or raises RpcError with a clear message.
    Accepts inputs with extra whitespace/quotes.
    """
    if not addr:
        raise RpcError("Empty contract address.")
    a = str(addr).strip().strip('"').strip("'")
    if a.startswith("0x") or a.startswith("0X"):
        h = a[2:]
    else:
        h = a
        a = "0x" + h
    if len(h) != 40 or not _HEX_RE.match(h):
        raise RpcError(f"Invalid contract address: {addr!r} (need 20-byte hex, e.g. 0x...40 hex chars)")
    return "0x" + h.lower()

def _rpc(method: str, params: list, timeout: int = 12, max_retries: int = 3, backoff_base: float = 0.5):
    if not RPC_URLS:
        raise RpcError("RPC_URL_OVERRIDE not set (you can pass multiple, comma-separated URLs).")
    last_err: Optional[Exception] = None
    for url in RPC_URLS:
        for attempt in range(max_retries):
            try:
                r = requests.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    timeout=timeout,
                )
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
                r.raise_for_status()
                j = r.json()
                if "error" in j:
                    code = j["error"].get("code")
                    if code in (-32005, -32000) or "rate" in str(j["error"]).lower():
                        raise RpcError(f"RPC throttled: {j['error']}")
                    raise RpcError(str(j["error"]))
                return j["result"]
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RpcError) as e:
                last_err = e
                time.sleep(min(8.0, backoff_base * (2 ** attempt)))
                continue
    raise RpcError(f"RPC failed across all endpoints after retries: {last_err}")

def _eth_call(to: str, data: str, block: Optional[int] = None) -> str:
    to_norm = normalize_contract(to)
    tag = hex(block) if isinstance(block, int) and block >= 0 else "latest"
    res = _rpc("eth_call", [{"to": to_norm, "data": data}, tag])
    return res or "0x"

def _decode_uint256(hex_data: str) -> int:
    if not hex_data or hex_data == "0x":
        return 0
    h = hex_data[2:] if hex_data.startswith("0x") else hex_data
    return int(h, 16)

def _decode_string(hex_data: str) -> str:
    if not hex_data or hex_data == "0x":
        return ""
    try:
        h = hex_data[2:] if hex_data.startswith("0x") else hex_data
        if len(h) < 128:
            chunk = h[-64:]
            return bytes.fromhex(chunk).rstrip(b"\x00").decode("utf-8", errors="ignore")
        length = int(h[64:128], 16)
        data = h[128:128 + length * 2]
        return bytes.fromhex(data).decode("utf-8", errors="ignore")
    except Exception:
        try:
            h = hex_data[2:] if hex_data.startswith("0x") else hex_data
            chunk = h[-64:]
            return bytes.fromhex(chunk).rstrip(b"\x00").decode("utf-8", errors="ignore")
        except Exception:
            return ""

ERC20_DECIMALS_SIG     = "0x313ce567"
ERC20_SYMBOL_SIG       = "0x95d89b41"
ERC20_TOTAL_SUPPLY_SIG = "0x18160ddd"

def erc20_decimals(contract: str, block: Optional[int] = None) -> int:
    out = _eth_call(contract, ERC20_DECIMALS_SIG, block)
    return _decode_uint256(out)

def erc20_symbol(contract: str, block: Optional[int] = None) -> str:
    out = _eth_call(contract, ERC20_SYMBOL_SIG, block)
    return _decode_string(out) or ""

def erc20_total_supply(contract: str, block: Optional[int] = None) -> int:
    out = _eth_call(contract, ERC20_TOTAL_SUPPLY_SIG, block)
    return _decode_uint256(out)

def fetch_metadata(contract: str, block: Optional[int] = None) -> dict:
    c = normalize_contract(contract)
    dec = erc20_decimals(c, block)
    sym = erc20_symbol(c, block)
    ts  = erc20_total_supply(c, block)
    return {
        "contract": c,
        "symbol": sym or "",
        "decimals": dec,
        "total_supply": ts,
        "as_of_block": block if isinstance(block, int) else None,
    }
