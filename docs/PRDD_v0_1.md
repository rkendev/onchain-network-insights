# PRDD v0.1 Onchain Network Insights

## Purpose
Provide a reliable baseline to ingest Ethereum data, compute small graph analytics, and render a minimal dashboard for inspection and anomaly triage.

## Scope for Sprint 1
Focus on block ingestion, token transfer edges, and a hundred node subgraph viewer. Leave GNN and advanced clustering for later.

## Actors
Solo developer
End user who needs a compact network view and basic metrics

## Functional Requirements
1. Ingest a contiguous block range given start and end and persist transactions and ERC20 transfers to SQLite. Success means a run of five hundred consecutive blocks completes with zero unhandled exceptions.
2. Build an address graph from ERC20 transfers with directed edges and compute degree and PageRank on a snapshot capped at ten thousand edges. Success means metrics are written to a table that contains address, in degree, out degree, pagerank.
3. Serve a small graph API that returns at most one hundred nodes and one hundred fifty edges per request and a time bounded query by block number. Success means an HTTP GET on graph snapshot returns JSON within two seconds on a local debug run.

## Nonfunctional Requirements
1. Reliability. If all RPC endpoints fail the ingestion run must not crash and must record a checkpoint to resume. Success means a failed provider test still leaves a resume file and the process exits with code zero.
2. Security. No plaintext API secrets in the repository. Success means pre commit hook blocks any file that contains a string that looks like an HTTP key or a hexadecimal key and CI fails on detection.
3. Testability. The project must include a smoke suite that runs under Python three dot twelve and finishes in under one minute on a fresh clone. Success means pytest reports at least five tests passed and no failures in CI.

## Data Model Summary
Addresses are nodes
Token transfers are directed edges with amount and timestamp
Blocks and transactions are base tables for traceability

## Metrics
Out degree per address
In degree per address
PageRank per address
Anomaly events with type and timestamp

## Interfaces
CLI command to ingest blocks and transfers
HTTP endpoint GET api graph snapshot at block equals N with a limit parameter

## Risks
Provider throttling or outages
Bursty graphs that exceed client rendering capacity

## Milestones
M1 baseline ingestion and storage completed and green in CI
M2 graph metrics job produces a snapshot and passes size limits
M3 basic graph endpoint and one page viewer return results under the two second budget
