package com.example.zmart;

/**
 * Central registry of all Kafka topic names used by the ZMart topology.
 *
 * <p>Keeping topic names as constants in one place means:
 * <ul>
 *   <li>Your topology and your tests always use the same string — no typos.
 *   <li>If you need to change a topic name, you change it in exactly one place.
 *   <li>Other microservices can depend on this module and use the same constants.
 * </ul>
 *
 * <p>Topic naming convention: {@code {domain}.{entity}.{qualifier}}
 */
public final class ZMartTopics {

    private ZMartTopics() {}

    // ── Input ─────────────────────────────────────────────────────────────────

    /** Raw purchases straight from POS systems. Contains unmasked credit card data. */
    public static final String PURCHASES_RAW          = "zmart.purchases.raw";

    /** Store reference data (compacted). Key = storeId. Loaded into GlobalKTable. */
    public static final String STORES                 = "zmart.stores";

    // ── Transformed / Masked ─────────────────────────────────────────────────

    /** Purchases with credit card masked. Safe to share with all consumers. */
    public static final String PURCHASES_MASKED       = "zmart.purchases.masked";

    /** Purchases enriched with store city/region (after GlobalKTable join). */
    public static final String PURCHASES_ENRICHED     = "zmart.purchases.enriched";

    // ── Department Branches ──────────────────────────────────────────────────

    /** Electronics department purchases only. */
    public static final String PURCHASES_ELECTRONICS  = "zmart.purchases.electronics";

    /** Clothing department purchases only. */
    public static final String PURCHASES_CLOTHING     = "zmart.purchases.clothing";

    /** All other department purchases (food, tools, etc.). */
    public static final String PURCHASES_OTHER        = "zmart.purchases.other";

    // ── Analytics ────────────────────────────────────────────────────────────

    /** Lightweight PurchasePattern records keyed by ZIP code. */
    public static final String PURCHASE_PATTERNS      = "zmart.purchase.patterns";

    // ── Stateful / Aggregated ─────────────────────────────────────────────────

    /** Running reward totals per customer (KTable changelog). */
    public static final String REWARDS                = "zmart.rewards";
}
