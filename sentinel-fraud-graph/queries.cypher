// ============================================================================
// Project SENTINEL — Neo4j Graph Fraud Ring & Shared-Device Detection Engine
// Complete Cypher Schema, Idempotent Batch Ingestion, & Forensic Traversal Suite
// ============================================================================

// ----------------------------------------------------------------------------
// 1. UNIQUENESS CONSTRAINTS & SUPPORTING INDEXES
// Guarantees O(log N) index-backed lookups during MERGE and prevents duplicate
// entity nodes across retryable stream micro-batches.
// ----------------------------------------------------------------------------

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


// ----------------------------------------------------------------------------
// 2. IDEMPOTENT UNWIND + MERGE BATCH LOADER (5,500+ TRANSACTIONS)
// Uses UNWIND $batch AS row with MERGE (never raw CREATE) on nodes and edges.
// Safe for at-least-once Kafka/Kinesis stream replay without duplicating state.
// ----------------------------------------------------------------------------

UNWIND $batch AS row
MERGE (c:Customer {customer_id: row.customer_id})
  ON CREATE SET
    c.home_city    = row.home_city,
    c.region       = row.region,
    c.customer_age = row.customer_age,
    c.account_age  = row.account_age,
    c.is_synthetic = true
  ON MATCH SET
    c.account_age  = row.account_age

MERGE (m:Merchant {merchant_id: row.merchant_id})
  ON CREATE SET
    m.synthetic_name    = row.merchant_name,
    m.merchant_category = row.merchant_category,
    m.city              = row.merchant_city,
    m.is_synthetic      = true

MERGE (c)-[p:PAID {transaction_id: row.transaction_id}]->(m)
  ON CREATE SET
    p.amount_paise       = row.amount_paise,
    p.transaction_amount = row.transaction_amount,
    p.payment_channel    = row.payment_channel,
    p.merchant_category  = row.merchant_category,
    p.transaction_time   = datetime(row.transaction_time),
    p.epoch_seconds      = row.epoch_seconds,
    p.device_id          = row.device_id,
    p.fraud_label        = row.fraud_label,
    p.status             = row.status,
    p.split              = row.split,
    p.reason_codes       = row.reason_codes
  ON MATCH SET
    p.fraud_label        = row.fraud_label,
    p.status             = row.status

FOREACH (_ IN CASE WHEN row.device_id IS NOT NULL THEN [1] ELSE [] END |
  MERGE (d:Device {device_id: row.device_id})
    ON CREATE SET
      d.device_kind  = row.device_kind,
      d.is_synthetic = true
  MERGE (c)-[u:USED_DEVICE]->(d)
    ON CREATE SET
      u.first_used_at = datetime(row.transaction_time),
      u.last_used_at  = datetime(row.transaction_time),
      u.first_epoch   = row.epoch_seconds,
      u.last_epoch    = row.epoch_seconds,
      u.txn_count     = 1
    ON MATCH SET
      u.first_used_at = CASE
        WHEN datetime(row.transaction_time) < u.first_used_at
        THEN datetime(row.transaction_time)
        ELSE u.first_used_at
      END,
      u.last_used_at  = CASE
        WHEN datetime(row.transaction_time) > u.last_used_at
        THEN datetime(row.transaction_time)
        ELSE u.last_used_at
      END,
      u.first_epoch   = CASE
        WHEN row.epoch_seconds < u.first_epoch THEN row.epoch_seconds ELSE u.first_epoch
      END,
      u.last_epoch    = CASE
        WHEN row.epoch_seconds > u.last_epoch THEN row.epoch_seconds ELSE u.last_epoch
      END
);


// ----------------------------------------------------------------------------
// 3. CYPHER TRAVERSAL 1: DEVICES SHARED BY >= 3 CUSTOMERS WITHIN A 7-DAY WINDOW
// Identifies hardware fingerprints rotated across 3 or more distinct customer
// accounts within 7 days (604,800 seconds), exposing coordinated mule farms.
// ----------------------------------------------------------------------------

MATCH (c1:Customer)-[p1:PAID]->(:Merchant),
      (c1)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(c2:Customer),
      (c2)-[p2:PAID]->(:Merchant)
WHERE p1.device_id = d.device_id
  AND p2.device_id = d.device_id
  AND c1.customer_id <> c2.customer_id
  AND abs(p1.epoch_seconds - p2.epoch_seconds) <= 7 * 86400
WITH d,
     p1,
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


// ----------------------------------------------------------------------------
// 4. CYPHER TRAVERSAL 2: CUSTOMERS WITHIN 2 HOPS OF A CONFIRMED FRAUD PAYMENT
// Starting from any Customer who executed a confirmed fraudulent payment
// (fraud_label = 1), traverses up to 2 hops across shared devices or shared
// merchants (`[:USED_DEVICE|PAID*1..2]`) to uncover accomplice accounts.
// ----------------------------------------------------------------------------

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


// ----------------------------------------------------------------------------
// 5. CYPHER TRAVERSAL 3: MERCHANTS MOST FREQUENTLY PAID VIA SHARED DEVICES
// Ranks merchants receiving payments from devices shared by >= 2 distinct
// customers, surfacing collusive cash-out endpoints and high-risk MCCs.
// ----------------------------------------------------------------------------

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


// ----------------------------------------------------------------------------
// 6. NEO4J BROWSER SUBGRAPH INSPECTION QUERY
// Interactive Cypher query for exploring the primary shared-device cluster.
// ----------------------------------------------------------------------------

MATCH (c:Customer)-[:USED_DEVICE]->(d:Device)
WITH d, count(DISTINCT c) AS shared_cust_count
WHERE shared_cust_count >= 4
WITH d ORDER BY shared_cust_count DESC LIMIT 5
MATCH path = (c:Customer)-[u:USED_DEVICE]->(d)<-[u2:USED_DEVICE]-(c2:Customer)-[p:PAID {fraud_label: 1}]->(m:Merchant)
RETURN path LIMIT 150;
