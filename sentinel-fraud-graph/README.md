# Project SENTINEL — Neo4j Graph Fraud Ring & Shared-Device Detection Engine

## 1. System Architecture & Engineering Context

High-throughput digital payment authorization platforms serving Tier-1 RBI-regulated payment aggregators process **4.2 million daily transactions** (~49 TPS average, 1,000 TPS sustained, **2,800 TPS peak**) across UPI (57.95%), Cards (31.02%), Wallets (6.97%), and Net Banking (4.06%) under strict real-time latency targets (**p50 <= 80 ms**, **p99 <= 250 ms**, hard fail-open cutoff at **300 ms**).

While point-in-time tabular gradient-boosted models evaluate individual payment attempts in isolation, coordinated fraud rings evade single-account velocity thresholds by distributing transactions across dozens of synthetic or compromised mule accounts (`Customer` nodes) that share a small pool of hardware device fingerprints (`Device` nodes) and cash out through collusive merchant terminals (`Merchant` nodes in `electronics`, `travel`, and `gaming`).

`sentinel_fraud_graph` implements the production **Neo4j Property-Graph Fraud Ring & Shared-Device Detection Engine** for Project SENTINEL:

```
+-----------------------------------------------------------------------------------+
|                   3-Tier Payment Telemetry & Privacy Gate                         |
|  Tier 1: Local CSV/Parquet -> Tier 2: PostgreSQL Replica -> Tier 3: Generator     |
|  enforce_data_privacy_guardrails() [DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Gate]   |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                Idempotent Batch Ingestion (`sentinel_fraud_graph.py`)             |
|  - Pooled Neo4j 5.x Bolt / AuraDB Driver (max_pool_size=50, timeout=5.0s)         |
|  - Uniqueness Constraints: Customer(customer_id), Device(device_id),              |
|    Merchant(merchant_id) + Relationship Indexes on PAID(transaction_id)           |
|  - At-Least-Once Safe UNWIND + MERGE Batch Loader (1,000 rows/batch, 5,500 txns)  |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|               Multi-Hop Cypher Fraud-Ring Detection & Analytics                   |
|  1. 7-Day Shared-Device Velocity (>= 3 distinct customers within 604,800s)        |
|  2. 2-Hop Fraud Proximity Traversal (`[:USED_DEVICE|PAID*1..2]` from fraud_label) |
|  3. Collusive Merchant Ranking via Shared Hardware Fingerprints (>= 2 customers)  |
|  4. Radial Subgraph Topology Renderer (`outputs/suspicious_cluster_graph.png`)    |
+-----------------------------------------------------------------------------------+
```

### Core Engineering Guarantees

* **DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Governance**: Every dataset ingested through `scripts/sentinel_data_loader.py` passes `enforce_data_privacy_guardrails()`, which strips 12 forbidden raw PII column types (`customer_name`, `full_name`, `email`, `phone`, `mobile_number`, `card_number`, `pan`, `aadhaar`, `upi_id`, `vpa`, `ip_address`, `bank_account_number`), enforces tokenized pseudonymous identifiers (`C_<16-hex>`, `M_<16-hex>`, `D_<16-hex>`), and verifies `is_synthetic == True` on 100% of records.
* **Index-Backed Idempotent Stream Ingestion**: Uniqueness constraints guarantee O(log N) index lookups during `MERGE` operations, preventing duplicate nodes and edges when upstream stream consumers replay transaction batches.
* **Dual-Mode Execution Resilience**: Supports live **Neo4j 5.x Community/Enterprise** via Docker Bolt (`bolt://localhost:7687`) and **Neo4j AuraDB** (`neo4j+s://...`), with an automatic deterministic in-memory property-graph engine (`--offline-mock`) for CI/CD regression pipelines.

## 2. Repository Layout

| Path | Description |
| :--- | :--- |
| `sentinel_fraud_graph.py` | Main production execution engine: driver initialization, privacy gate, constraint creation, idempotent `MERGE` batch loader, Cypher traversal runner, subgraph renderer, and automated verification gates |
| `scripts/verify_supabase_ingestion.py` | Supabase data ingestion verification script: connects via `DATABASE_URL`, queries customer, merchant, and paid transaction record counts, and validates ingestion |
| `queries.cypher` | Standalone Cypher reference suite containing uniqueness constraints, indexes, parameterized batch `MERGE` statement, and the 3 multi-hop fraud-ring detection queries |
| `scripts/sentinel_data_loader.py` | 3-Tier telemetry loader and `enforce_data_privacy_guardrails()` validator |
| `docker-compose.yml` | Container specification for `neo4j:5.20-community` exposing HTTP (`7474`) and Bolt (`7687`) with APOC enabled, heap/pagecache memory tuning, and healthcheck probes |
| `.env.example` | Sanitized environment configuration template for Local Docker Bolt, Neo4j AuraDB, connection pooling, and telemetry generator parameters |
| `.gitignore` | Hardened secret and data-dump exclusion rules (`.env`, private keys, raw data directories, and binary dumps blocked) |
| `requirements.txt` | Python runtime dependencies (`neo4j>=5.20.0`, `pandas>=2.0.0`, `numpy>=1.24.0`, `matplotlib>=3.7.0`, `python-dotenv>=1.0.0`, `tabulate>=0.9.0`) |
| `outputs/suspicious_cluster_graph.png` | High-resolution radial topology visualization of the primary shared-device fraud ring (`24` nodes, `51` edges) |
| `outputs/graph_execution_audit.json` | Machine-readable execution audit containing constraint status, node/edge counts, idempotency replay proof, and query benchmarks |
| `outputs/query_1_shared_devices_7d.csv` | Exported results for Query 1: Devices shared by >= 3 customers within a 7-day window |
| `outputs/query_2_two_hop_fraud_customers.csv` | Exported results for Query 2: Customers within 2 hops of a confirmed fraudulent payment |
| `outputs/query_3_merchants_shared_devices.csv` | Exported results for Query 3: Merchants most frequently paid via shared devices |

## 3. Property-Graph Schema & Data Flow

### Node Entities & Uniqueness Constraints

| Label | Primary Key | Uniqueness Constraint | Node Properties | Cardinality (Benchmark Run) |
| :--- | :--- | :--- | :--- | :--- |
| `:Customer` | `customer_id` (`C_<16-hex>`) | `customer_id_unique` | `home_city`, `region`, `customer_age`, `account_age`, `is_synthetic` | **500** nodes |
| `:Device` | `device_id` (`D_<16-hex>`) | `device_id_unique` | `device_kind` (`mobile_android`, `mobile_ios`, `desktop_web`), `is_synthetic` | **899** nodes |
| `:Merchant` | `merchant_id` (`M_<16-hex>`) | `merchant_id_unique` | `synthetic_name` (`Merchant_<MCC>_<id>`), `merchant_category`, `city`, `is_synthetic` | **120** nodes |

### Relationship Schema

| Relationship Pattern | Identity Key | Properties Maintained | Cardinality (Benchmark Run) |
| :--- | :--- | :--- | :--- |
| `(:Customer)-[:USED_DEVICE]->(:Device)` | `(customer_id, device_id)` | `first_used_at`, `last_used_at`, `first_epoch`, `last_epoch`, `txn_count` | **1,846** edges |
| `(:Customer)-[:PAID]->(:Merchant)` | `transaction_id` (`TX_<10-digit>`) | `transaction_id`, `amount_paise` (integer paise = round(amount * 100)), `transaction_amount`, `payment_channel`, `merchant_category`, `transaction_time`, `epoch_seconds`, `device_id`, `fraud_label` (`0`/`1`), `status`, `split`, `reason_codes` | **5,500** edges |

### Uniqueness Constraint & Index DDL (`queries.cypher`)

```cypher
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;

CREATE CONSTRAINT device_id_unique IF NOT EXISTS
FOR (d:Device) REQUIRE d.device_id IS UNIQUE;

CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS
FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE;

CREATE INDEX paid_transaction_id_idx IF NOT EXISTS
FOR ()-[p:PAID]-() ON (p.transaction_id);

CREATE INDEX paid_fraud_label_idx IF NOT EXISTS
FOR ()-[p:PAID]-() ON (p.fraud_label);
```

### Idempotent `UNWIND` + `MERGE` Batch Ingestion Design

In distributed streaming architectures (Kafka / Kinesis -> Neo4j sink), network partitions and consumer rebalances trigger at-least-once batch redelivery. Using raw `CREATE` statements duplicates nodes and relationships on replay, distorting graph degree features (`distinct_customers_per_device_7d`).

By combining uniqueness constraints with `UNWIND` batch parameters and `MERGE` clauses keyed on primary identifiers (`customer_id`, `device_id`, `merchant_id`, and `transaction_id`), re-ingesting any slice of transactions updates mutable state in place without creating duplicate graph structure:

* **Initial Ingestion (5,500 Transactions across 6 micro-batches of 1,000 rows)**:
  * **Total Nodes**: `1,519` (`500` `:Customer`, `899` `:Device`, `120` `:Merchant`)
  * **Total Relationships**: `7,346` (`1,846` `:USED_DEVICE`, `5,500` `:PAID`)
* **Replay Verification (Re-ingesting 500 Transactions via `MERGE`)**:
  * **Node Count Delta**: `delta_nodes = 0` (`1,519 -> 1,519`)
  * **Relationship Count Delta**: `delta_edges = 0` (`7,346 -> 7,346`)

## 4. Setup, Deployment & Verification Instructions

### Prerequisites

* Python `3.10+`
* Docker & Docker Compose (optional if connecting to Neo4j AuraDB or running `--offline-mock` verification)

### Step 1: Install Dependencies & Configure Environment

```bash
cp .env.example .env
pip install -r requirements.txt
```

#### (Optional) Verify Supabase Telemetry Replica Ingestion

```bash
python scripts/verify_supabase_ingestion.py
```
*Queries the Supabase feature store (`DATABASE_URL`), verifying record counts for `customers`, `merchants`, and `paid transactions` (`transactions` joined with `payment_outcomes`).*


### Step 2: Start Neo4j via Docker Compose

```bash
docker compose up -d
docker compose ps
```

* **Neo4j Browser UI**: `http://localhost:7474`
* **Bolt Binary Endpoint**: `bolt://localhost:7687`
* **Default Credentials**: `neo4j` / `<your-neo4j-password>` (or `${NEO4J_PASSWORD:-neo4j_local_dev_only}`)

### Step 3: Execute Ingestion, Traversals & Automated Verification Gates

```bash
# Execute against live Neo4j Bolt / AuraDB (automatically falls back if daemon is unreachable)
python3 sentinel_fraud_graph.py --verify

# Execute deterministic CI/CD verification in offline property-graph mode
python3 sentinel_fraud_graph.py --offline-mock --verify
```

### Automated Verification Gates

Running `python3 sentinel_fraud_graph.py --verify` validates 6 production readiness gates (`outputs/graph_execution_audit.json`):

1. `Gate 1 (DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Guardrail)`: Confirms 12 PII column types are absent/stripped, merchant names are tokenized, and 100% of records carry `is_synthetic == True`.
2. `Gate 2 (Uniqueness Constraints Enforced on Customer, Device, Merchant)`: Confirms `customer_id_unique`, `device_id_unique`, and `merchant_id_unique` constraints and relationship indexes are active.
3. `Gate 3 (>= 5,000 Transactions Loaded & MERGE Idempotency Proven)`: Confirms `5,500` `:PAID` edges are ingested and replaying a 500-transaction batch produces `delta_nodes == 0` and `delta_edges == 0`.
4. `Gate 4 (Query 1 Detects Shared Mule Devices >= 3 Customers in 7d)`: Confirms sliding 7-day device-sharing traversal identifies coordinated hardware hubs.
5. `Gate 5 (Query 2 & Query 3 Detect 2-Hop Suspects & Collusive Merchants)`: Confirms 2-hop fraud proximity and shared-device merchant rankings return validated risk tables.
6. `Gate 6 (Suspicious Cluster Visualization Rendered & Non-Empty)`: Confirms `outputs/suspicious_cluster_graph.png` is rendered at 220 DPI.

## 5. Multi-Hop Fraud-Ring Query Benchmarks & Forensic Findings

### Query 1: Devices Shared by >= 3 Distinct Customers Within a 7-Day Window

Identifies hardware device fingerprints rotated across 3 or more distinct customer accounts within a 7-day (`604,800` second) window (`FE-09: distinct_customers_per_device_7d`).

```cypher
MATCH (c1:Customer)-[p1:PAID]->(:Merchant),
      (c1)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(c2:Customer),
      (c2)-[p2:PAID]->(:Merchant)
WHERE p1.device_id = d.device_id
  AND p2.device_id = d.device_id
  AND c1.customer_id <> c2.customer_id
  AND abs(p1.epoch_seconds - p2.epoch_seconds) <= 7 * 86400
WITH d,
     collect(DISTINCT c1.customer_id) + collect(DISTINCT c2.customer_id) AS window_customers
UNWIND window_customers AS cid
WITH d,
     count(DISTINCT cid) AS distinct_customers_7d,
     collect(DISTINCT cid)[0..8] AS sample_customer_ids
WHERE distinct_customers_7d >= 3
MATCH (c:Customer)-[p:PAID]->(:Merchant)
WHERE p.device_id = d.device_id
RETURN d.device_id AS device_id,
       d.device_kind AS device_kind,
       distinct_customers_7d,
       count(DISTINCT p.transaction_id) AS total_device_txns,
       sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_txns,
       round(sum(p.transaction_amount), 2) AS total_exposure_inr,
       sample_customer_ids
ORDER BY distinct_customers_7d DESC, confirmed_fraud_txns DESC, total_exposure_inr DESC
LIMIT 15;
```

#### Benchmark Results — Top 10 Shared Mule Devices (`outputs/query_1_shared_devices_7d.csv`)

| Device ID | Device Kind | Distinct Customers (7d) | Total Device Txns | Confirmed Fraud Txns | Total Exposure (INR) | Sample Linked Customer IDs |
| :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| `D_b64f832752bf8208` | `desktop_web` | 28 | 32 | 30 | Rs. 13,64,964.33 | `C_08b8ffde55aee343`, `C_0ccbea32a85d6241`, `C_203e48776e04dc91` |
| `D_4719e96e57524728` | `mobile_ios` | 27 | 37 | 34 | Rs. 13,31,649.04 | `C_054319d45929712a`, `C_0687b872d71438a8`, `C_0ccbea32a85d6241` |
| `D_8bc56ee8a19df557` | `desktop_web` | 26 | 34 | 29 | Rs. 9,45,512.28 | `C_0e165ded2d945eb1`, `C_105d9a98de65fb18`, `C_1314e6a50965b967` |
| `D_1316c0b28d5165c3` | `mobile_ios` | 24 | 33 | 30 | Rs. 11,57,890.05 | `C_0b4df1a1818d5098`, `C_0b6c61e6b6d3d0df`, `C_203e48776e04dc91` |
| `D_6f9bfcd66777d915` | `desktop_web` | 24 | 30 | 25 | Rs. 9,28,719.34 | `C_054319d45929712a`, `C_0687b872d71438a8`, `C_0e165ded2d945eb1` |
| `D_5ca245340c743a27` | `desktop_web` | 23 | 35 | 27 | Rs. 13,45,682.41 | `C_0ccbea32a85d6241`, `C_27c7e9189409bb1a`, `C_3069276b407e98c8` |
| `D_99d79345f2dd3ca2` | `desktop_web` | 23 | 31 | 24 | Rs. 9,95,406.84 | `C_08fc8ae9fedc2f40`, `C_0b4df1a1818d5098`, `C_0cc9cb709b07eae1` |
| `D_76e1bf74342f99af` | `mobile_android` | 22 | 26 | 22 | Rs. 9,71,728.33 | `C_105d9a98de65fb18`, `C_1c524692d5af9e23`, `C_26beb2db86c5543c` |
| `D_0bb69097ee9cdc43` | `mobile_android` | 21 | 27 | 22 | Rs. 9,13,766.93 | `C_011e537fa1d83872`, `C_0687b872d71438a8`, `C_0ccbea32a85d6241` |
| `D_6d7f5ab50a9d0d70` | `desktop_web` | 20 | 30 | 25 | Rs. 11,88,641.92 | `C_0278915293e2dc5e`, `C_054319d45929712a`, `C_0b4df1a1818d5098` |

* **Engineering Analysis**: Across the top 10 shared devices, **83.3% to 93.8%** of payment authorizations are confirmed fraud (`fraud_label = 1`). Enforcing a real-time velocity rule or feature gate on `distinct_customers_per_device_7d >= 3` isolates emulator farms and credential-stuffing infrastructure with minimal false positives on genuine single-user devices.

### Query 2: Customers Within 2 Hops of a Confirmed Fraudulent Payment

Traverses up to 2 hops (`[:USED_DEVICE|PAID*1..2]`) from any customer with a confirmed fraudulent transaction (`fraud_label = 1`) to identify accomplice accounts sharing hardware devices or collusive merchant endpoints.

```cypher
MATCH (fraud_c:Customer)-[fraud_p:PAID {fraud_label: 1}]->(target_m:Merchant)
MATCH path = (fraud_c)-[:USED_DEVICE|PAID*1..2]-(suspect_c:Customer)
WHERE suspect_c.customer_id <> fraud_c.customer_id
OPTIONAL MATCH (fraud_c)-[:USED_DEVICE]->(shared_d:Device)<-[:USED_DEVICE]-(suspect_c)
WITH suspect_c,
     count(DISTINCT fraud_c.customer_id) AS linked_fraud_customers,
     count(DISTINCT shared_d.device_id) AS shared_mule_devices,
     count(DISTINCT target_m.merchant_id) AS shared_fraud_merchants,
     collect(DISTINCT shared_d.device_id)[0..5] AS bridge_devices
MATCH (suspect_c)-[sp:PAID]->(:Merchant)
RETURN suspect_c.customer_id AS suspect_customer_id,
       suspect_c.home_city AS home_city,
       linked_fraud_customers,
       shared_mule_devices,
       shared_fraud_merchants,
       count(DISTINCT sp.transaction_id) AS suspect_total_txns,
       sum(CASE WHEN sp.fraud_label = 1 THEN 1 ELSE 0 END) AS suspect_fraud_txns,
       round(sum(sp.transaction_amount), 2) AS suspect_spend_inr,
       bridge_devices
ORDER BY shared_mule_devices DESC, linked_fraud_customers DESC, suspect_spend_inr DESC
LIMIT 20;
```

#### Benchmark Results — Top 10 2-Hop Linked Suspect Accounts (`outputs/query_2_two_hop_fraud_customers.csv`)

| Suspect Customer ID | Home City | Linked Fraud Customers | Shared Mule Devices | Shared Fraud Merchants | Total Txns | Fraud Txns | Suspect Spend (INR) | Sample Bridge Devices |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `C_c2029a5f382793db` | Bengaluru | 201 | 14 | 19 | 28 | 12 | Rs. 6,46,652.38 | `D_1316c0b28d5165c3`, `D_15aad262671ffd44` |
| `C_b4c6d52359b64cee` | Chennai | 176 | 13 | 13 | 23 | 11 | Rs. 5,04,197.00 | `D_15aad262671ffd44`, `D_2009582bd89761fe` |
| `C_4a17b7185c5215b6` | Pune | 189 | 10 | 12 | 17 | 8 | Rs. 4,80,025.83 | `D_1316c0b28d5165c3`, `D_4719e96e57524728` |
| `C_549b6851bf65034f` | Kolkata | 169 | 10 | 11 | 15 | 10 | Rs. 5,80,784.33 | `D_15aad262671ffd44`, `D_5ca245340c743a27` |
| `C_c888fbd6264b01c6` | Delhi | 165 | 10 | 15 | 23 | 12 | Rs. 5,75,487.01 | `D_1316c0b28d5165c3`, `D_2009582bd89761fe` |
| `C_3e091a159cff9b82` | Mumbai | 163 | 10 | 12 | 19 | 12 | Rs. 6,62,211.09 | `D_0bb69097ee9cdc43`, `D_1316c0b28d5165c3` |
| `C_105d9a98de65fb18` | Delhi | 160 | 10 | 7 | 13 | 9 | Rs. 1,79,843.41 | `D_2009582bd89761fe`, `D_4719e96e57524728` |
| `C_0687b872d71438a8` | Kolkata | 175 | 9 | 13 | 18 | 7 | Rs. 4,30,856.50 | `D_0bb69097ee9cdc43`, `D_15aad262671ffd44` |
| `C_bdb2b574454bab95` | Ahmedabad | 168 | 9 | 13 | 19 | 8 | Rs. 5,05,788.33 | `D_5ca245340c743a27`, `D_6d7f5ab50a9d0d70` |
| `C_27c7e9189409bb1a` | Mumbai | 156 | 9 | 9 | 13 | 7 | Rs. 4,48,937.99 | `D_0bb69097ee9cdc43`, `D_1316c0b28d5165c3` |

* **Engineering Analysis**: Because chargeback and dispute labels mature up to 60 days after authorization (`label_available_at = transaction_time + 60 days`), sleeper accounts in an active fraud ring often have zero matured fraud labels at scoring time. Ranking 2-hop neighbors by `shared_mule_devices` and `linked_fraud_customers` exposes linked accounts before chargebacks materialize.

### Query 3: Merchants Most Frequently Paid via Shared Devices (`>= 2` Customers)

Identifies merchant terminals receiving disproportionate payment volume from devices shared by 2 or more distinct customers.

```cypher
MATCH (c:Customer)-[:USED_DEVICE]->(d:Device)
WITH d, count(DISTINCT c.customer_id) AS device_customer_degree
WHERE device_customer_degree >= 2
MATCH (payer:Customer)-[p:PAID]->(m:Merchant)
WHERE p.device_id = d.device_id
RETURN m.merchant_id AS merchant_id,
       m.synthetic_name AS merchant_name,
       m.merchant_category AS merchant_category,
       count(DISTINCT p.transaction_id) AS shared_device_txns,
       count(DISTINCT d.device_id) AS distinct_shared_devices,
       count(DISTINCT payer.customer_id) AS distinct_shared_customers,
       sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_txns,
       round(100.0 * sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) / count(p), 2) AS fraud_rate_pct,
       round(sum(p.transaction_amount), 2) AS shared_device_volume_inr
ORDER BY shared_device_txns DESC, confirmed_fraud_txns DESC
LIMIT 15;
```

#### Benchmark Results — Top 10 Shared-Device Target Merchants (`outputs/query_3_merchants_shared_devices.csv`)

| Merchant ID | Merchant Name | MCC Category | Shared-Device Txns | Distinct Shared Devices | Distinct Shared Customers | Confirmed Fraud Txns | Fraud Rate (%) | Shared-Device Volume (INR) |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `M_b15c5e4c19a34cc6` | `Merchant_Electronics_028` | `electronics` | 76 | 44 | 60 | 48 | 63.16% | Rs. 20,46,665.84 |
| `M_91fd430abec5a213` | `Merchant_Electronics_021` | `electronics` | 74 | 36 | 63 | 54 | 72.97% | Rs. 22,01,725.89 |
| `M_284df6b5c8a360ae` | `Merchant_Travel_032` | `travel` | 68 | 41 | 60 | 42 | 61.76% | Rs. 18,26,968.38 |
| `M_f4caa346bafafd3e` | `Merchant_Travel_033` | `travel` | 65 | 41 | 55 | 40 | 61.54% | Rs. 14,56,717.53 |
| `M_fbc56865d6f27b2d` | `Merchant_Travel_020` | `travel` | 62 | 28 | 54 | 43 | 69.35% | Rs. 17,56,328.10 |
| `M_e9c05049527a28f5` | `Merchant_Travel_015` | `travel` | 61 | 34 | 55 | 47 | 77.05% | Rs. 17,10,563.22 |
| `M_eaddf6ba969de66d` | `Merchant_Travel_034` | `travel` | 61 | 34 | 53 | 42 | 68.85% | Rs. 16,81,837.61 |
| `M_18bae063a825a759` | `Merchant_Travel_013` | `travel` | 61 | 33 | 53 | 41 | 67.21% | Rs. 14,60,875.62 |
| `M_a009f012be7c0d67` | `Merchant_Gaming_001` | `gaming` | 59 | 40 | 50 | 40 | 67.80% | Rs. 13,92,204.47 |
| `M_fec01df425aaf906` | `Merchant_Gaming_027` | `gaming` | 47 | 28 | 44 | 35 | 74.47% | Rs. 13,50,699.84 |

* **Engineering Analysis**: Cash-out activity from shared devices clusters heavily in `electronics`, `travel`, and `gaming` merchants (`R08_HIGH_RISK_MCC`), where fraud rates on shared-device transactions reach **61.54% to 77.05%**.

## 6. Suspicious Cluster Subgraph Topology (`outputs/suspicious_cluster_graph.png`)

The automated visualization pipeline renders the primary detected fraud ring (`24` nodes, `51` edges) into a 3-tier radial layout (`outputs/suspicious_cluster_graph.png`):

![Suspicious Cluster Subgraph Topology](outputs/suspicious_cluster_graph.png)

* **Inner Core (Red Hexagons — `n = 4`)**: The highest-velocity shared mule devices (`D_b64f832752bf8208`, `D_4719e96e57524728`, `D_8bc56ee8a19df557`, `D_1316c0b28d5165c3`), each shared across 24 to 28 distinct customer accounts within 7 days.
* **Middle Ring (Orange & Gold Circles — `n = 13`)**: `12` confirmed fraud customer accounts and `1` 2-hop linked suspect customer account (`C_871d59..` in Chennai) connected to the inner-core device hubs via solid amber `[:USED_DEVICE]` edges.
* **Outer Ring (Blue Squares — `n = 7`)**: The top targeted high-risk merchants (`Merchant_Electronics_028`, `Merchant_Electronics_021`, `Merchant_Travel_020`, `Merchant_Travel_034`, `Merchant_Travel_033`, `Merchant_Travel_015`, `Merchant_Travel_032`) receiving dashed red `[:PAID {fraud_label: 1}]` payouts.
