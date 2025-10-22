import importlib

def test_core_modules_and_symbols_exist():
    mods = [
        ("common.settings", ["load_settings"]),
        ("ingestion.fetcher", ["fetch_block", "fetch_transaction"]),
        ("storage.sqlite_backend", ["SQLiteStorage"]),
    ]
    for mod_name, symbols in mods:
        mod = importlib.import_module(mod_name)
        for sym in symbols:
            assert hasattr(mod, sym), f"{mod_name} missing {sym}"
