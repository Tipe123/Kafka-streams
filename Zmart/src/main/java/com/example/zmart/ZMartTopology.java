package com.example.zmart;

import com.example.zmart.model.Purchase;
import com.example.zmart.model.PurchasePattern;
import com.example.zmart.model.RewardAccumulator;
import com.example.zmart.model.Store;
import com.example.zmart.serde.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The complete ZMart Kafka Streams topology.
 *
 * <h2>Topology overview (DAG)</h2>
 * <pre>
 * SOURCE: zmart.purchases.raw
 *     │
 *     ├─[1] mapValues(maskCreditCard)     ── stateless ──────────────────────┐
 *     │        │                                                              │
 *     │        ├─[2] filter(total > 0)    ── stateless                       │
 *     │        │        │                                                     │
 *     │        │        ├─[3] split/branch                                   │
 *     │        │        │       ├─ dept=ELECTRONICS ──► zmart.purchases.electronics
 *     │        │        │       ├─ dept=CLOTHING    ──► zmart.purchases.clothing
 *     │        │        │       └─ default          ──► zmart.purchases.masked
 *     │        │        │                                                     │
 *     │        │        └─[4] mapValues(PurchasePattern::from) ── stateless  │
 *     │        │                        │                                     │
 *     │        │                        └─────────────► zmart.purchase.patterns
 *     │        │                                                              │
 *     │        └─[5] selectKey(customerId)  ── marks need for repartition    │
 *     │                 │                                                     │
 *     │                 └─[6] groupByKey().aggregate(RewardAccumulator) ── STATEFUL
 *     │                                   │                                   │
 *     │                                   └───────────► zmart.rewards        │
 *     │                                                                       │
 *     └─[7] join(storeGlobalKTable)       ── stateless (no repartition) ─────┘
 *                │
 *                └─────────────────────────► zmart.purchases.enriched
 * </pre>
 *
 * <h2>Stateless vs Stateful</h2>
 * <ul>
 *   <li>Steps 1-4, 7: stateless — process each record in isolation, no RocksDB</li>
 *   <li>Step 6: stateful — maintains per-customerID reward accumulator in RocksDB</li>
 * </ul>
 *
 * <h2>Repartitioning</h2>
 * Step 5 ({@code selectKey}) changes the key from the original POS-assigned key
 * to {@code customerId}. This marks the stream as needing repartitioning. Kafka
 * Streams will automatically insert an internal repartition topic before step 6's
 * {@code groupByKey()} so that all records for the same customer land on the same
 * partition (and hence the same stream task/thread).
 */
public class ZMartTopology {

    private static final Logger log = LoggerFactory.getLogger(ZMartTopology.class);

    // ── State store names ─────────────────────────────────────────────────────
    // These become the RocksDB store name AND the suffix for the changelog topic:
    //   {appId}-{storeName}-changelog
    public static final String REWARDS_STORE       = "rewards-store";
    public static final String STORE_GLOBAL_STORE  = "store-global-store";

    // -------------------------------------------------------------------------
    // Main build method
    // -------------------------------------------------------------------------

    /**
     * Builds and returns the full ZMart topology.
     *
     * <p>Accepts a {@link StreamsBuilder} so that tests (using
     * {@code TopologyTestDriver}) and the main application share the exact same
     * topology-building logic. Tests call this with a fresh builder; production
     * calls it once at startup.
     *
     * @param builder the StreamsBuilder to mutate
     * @return the built {@link Topology}, ready to pass to {@link org.apache.kafka.streams.KafkaStreams}
     */
    public static Topology build(StreamsBuilder builder) {

        // ── [0] Load store reference data into a GlobalKTable ─────────────────
        //
        // GlobalKTable = every instance of this application loads the FULL table.
        // This allows foreign-key joins (join on storeId even though the stream is
        // keyed by customerId) without triggering a repartition.
        //
        // The compacted topic zmart.stores is the source of truth for store metadata.
        GlobalKTable<String, Store> storeTable = builder.globalTable(
                ZMartTopics.STORES,
                Consumed.with(Serdes.String(), JsonSerdes.store()),
                Materialized.as(STORE_GLOBAL_STORE)
        );

        // ── [1] Source: read raw purchases ────────────────────────────────────
        //
        // At this point the key could be anything set by the producer (e.g. purchaseId).
        // We will change it to customerId in step 5.
        KStream<String, Purchase> rawStream = builder.stream(
                ZMartTopics.PURCHASES_RAW,
                Consumed.with(Serdes.String(), JsonSerdes.purchase())
        );

        // ── [1] Mask the credit card number ───────────────────────────────────
        //
        // mapValues: transforms only the value, key is unchanged → NO repartition.
        //
        // This step runs FIRST and on EVERY record so that ALL downstream branches
        // (electronics, clothing, rewards, patterns) always see masked data.
        // The raw credit card number is never written to any output topic.
        KStream<String, Purchase> maskedStream = rawStream
                .mapValues(ZMartTopology::maskCreditCard)
                .peek((k, v) -> log.debug("[MASK] key={} cc={}", k, v.getCreditCardNumber()));

        // ── [2] Filter: drop invalid / zero-value records ─────────────────────
        //
        // filter: records failing the predicate are dropped — they never reach
        // downstream nodes. This is purely stateless.
        KStream<String, Purchase> validStream = maskedStream
                .filter((key, purchase) -> purchase.getPurchaseTotal() > 0.00
                        && purchase.getCustomerId() != null);

        // ── [3] Branch: route to department-specific sinks ────────────────────
        //
        // split().branch() sends each record to AT MOST ONE branch (first match wins).
        // Records that don't match any named branch go to the defaultBranch.
        //
        // Each branch returns a separate KStream — you can process them
        // independently. Here we write each one to its own topic so that
        // department-specific consumers subscribe to only what they care about.
        Map<String, KStream<String, Purchase>> departmentBranches = validStream
                .split(Named.as("dept-"))
                .branch(
                        (k, v) -> "ELECTRONICS".equalsIgnoreCase(v.getDepartment()),
                        Branched.as("electronics")
                )
                .branch(
                        (k, v) -> "CLOTHING".equalsIgnoreCase(v.getDepartment()),
                        Branched.as("clothing")
                )
                .defaultBranch(Branched.as("other"));

        KStream<String, Purchase> electronicsStream = departmentBranches.get("dept-electronics");
        KStream<String, Purchase> clothingStream    = departmentBranches.get("dept-clothing");
        KStream<String, Purchase> otherStream       = departmentBranches.get("dept-other");

        // Sink: write each department to its own topic
        electronicsStream.to(ZMartTopics.PURCHASES_ELECTRONICS,
                Produced.with(Serdes.String(), JsonSerdes.purchase()));

        clothingStream.to(ZMartTopics.PURCHASES_CLOTHING,
                Produced.with(Serdes.String(), JsonSerdes.purchase()));

        // "other" goes to the general masked topic (safe for all consumers)
        otherStream.to(ZMartTopics.PURCHASES_MASKED,
                Produced.with(Serdes.String(), JsonSerdes.purchase()));

        // ── [4] Extract purchase patterns (analytics) ─────────────────────────
        //
        // mapValues: project the Purchase into a lightweight PurchasePattern.
        // Keyed by the original key (whatever the producer set) — which is fine
        // because analytics consumers don't need per-customer grouping here.
        // In production you might selectKey(zipCode) first for partition locality.
        validStream
                .mapValues(PurchasePattern::from)
                .peek((k, v) -> log.debug("[PATTERN] zip={} item={}", v.getZipCode(), v.getItemPurchased()))
                .to(ZMartTopics.PURCHASE_PATTERNS,
                        Produced.with(Serdes.String(), JsonSerdes.purchasePattern()));

        // ── [5 + 6] Rewards accumulation (STATEFUL) ───────────────────────────
        //
        // Key change: selectKey(customerId) so that all purchases by the same
        // customer go to the same partition and are handled by the same task.
        //
        // Why selectKey and not map()?
        //   selectKey is semantically clearer (intent: change key only).
        //   map() also changes the key but is more general (also changes value).
        //   Both mark the stream as needing repartition before groupByKey.
        //
        // groupByKey() uses the key as-is (customerId now). No additional repartition.
        //
        // aggregate():
        //   - Initializer: RewardAccumulator::empty — called once for a new key
        //   - Aggregator:  acc.update(purchase) — called for each subsequent record
        //   - Materialized: names the RocksDB store that holds the running totals
        //
        // The result is a KTable<customerId, RewardAccumulator>.
        // KTable semantics: the "current" reward balance for each customer.
        KTable<String, RewardAccumulator> rewardsTable = validStream
                .selectKey((origKey, purchase) -> purchase.getCustomerId())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.purchase()))
                .aggregate(
                        RewardAccumulator::empty,
                        (customerId, purchase, accumulator) -> accumulator.update(purchase),
                        Materialized.<String, RewardAccumulator, KeyValueStore<Bytes, byte[]>>
                                as(REWARDS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.rewardAccumulator())
                );

        // Convert KTable to KStream so we can write to a topic.
        // toStream() emits a record every time a value for a key is updated.
        rewardsTable.toStream()
                .peek((k, v) -> log.info("[REWARDS] {} → {} pts (${} total)",
                        k, v.getRewardPoints(), String.format("%.2f", v.getTotalPurchases())))
                .to(ZMartTopics.REWARDS,
                        Produced.with(Serdes.String(), JsonSerdes.rewardAccumulator()));

        // ── [7] Store enrichment join (GlobalKTable) ──────────────────────────
        //
        // Join the masked/valid stream with the store GlobalKTable.
        //
        // join() parameters:
        //   1. The GlobalKTable to join with
        //   2. KeyValueMapper: extracts the join key from the stream record
        //      Here: purchase.getStoreId() — this is the "foreign key"
        //      (the stream key is customerId, but we want to look up by storeId)
        //   3. ValueJoiner: combines the matched purchase + store → enriched purchase
        //
        // Because it's a GlobalKTable (not a KTable), there is NO repartition
        // even though purchase.getStoreId() may differ from the stream key.
        // Every stream instance has all store records locally.
        maskedStream.join(
                storeTable,
                (purchaseKey, purchase) -> purchase.getStoreId(),   // foreign key extractor
                (purchase, store) -> Purchase.builder(purchase)
                        .storeRegion(store.getRegion())
                        .storeCity(store.getCity())
                        .build()
        ).to(ZMartTopics.PURCHASES_ENRICHED,
                Produced.with(Serdes.String(), JsonSerdes.purchase()));

        return builder.build();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helper methods (pure functions — easy to unit-test independently)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Masks all but the last 4 digits of a credit card number.
     *
     * <p>Examples:
     * <ul>
     *   <li>"4532-1234-5678-9101" → "XXXX-XXXX-XXXX-9101"
     *   <li>"4532123456789101"    → "XXXX-XXXX-XXXX-9101"
     *   <li>null / "" → unchanged
     * </ul>
     *
     * <p>This method is intentionally {@code static} so it can be referenced
     * as a method reference ({@code ZMartTopology::maskCreditCard}) and tested
     * independently without building a full topology.
     */
    static Purchase maskCreditCard(Purchase purchase) {
        String raw = purchase.getCreditCardNumber();
        if (raw == null || raw.isBlank()) {
            return purchase; // nothing to mask
        }
        String digitsOnly = raw.replaceAll("[^0-9]", ""); // strip dashes, spaces
        if (digitsOnly.length() < 4) {
            return purchase; // too short to mask safely
        }
        String last4  = digitsOnly.substring(digitsOnly.length() - 4);
        String masked = "XXXX-XXXX-XXXX-" + last4;

        return Purchase.builder(purchase)
                .creditCardNumber(masked)
                .build();
    }
}
