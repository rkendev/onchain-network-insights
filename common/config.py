# config.yaml
data_source: rpc

bigquery:
  project_id: your-project-id
  dataset: ethereum

rpc:
  url: "https://mainnet.infura.io/v3/276f912c1d174836b7865519f2bb9fe6"  # override via $RPC_URL_OVERRIDE at runtime

api:
  etherscan_key: "REDACTED"

checkpoint:
  file: checkpoint.json
