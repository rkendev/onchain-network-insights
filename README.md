# onchain-network-insights

## Description  
On-chain network analytics & forensic exploration of token flows and wallet behavior.

## Structure  
- `ingestion/`: data fetching & parsing  
- `common/`: shared utilities, config, logging  
- `tests/`: unit tests  
- `.github/workflows/`: CI workflows  
- `venv/`: local Python environment  

## Getting Started  
```bash
bash scripts/bootstrap.sh
source venv/bin/activate
pytest  # run test suite
