package com.example.zmart;

import com.example.zmart.model.Purchase;
import com.example.zmart.model.PurchasePattern;
import com.example.zmart.model.RewardAccumulator;
import com.example.zmart.model.Store;
import com.example.zmart.serde.JsonDeserializer;
import com.example.zmart.serde.JsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link ZMartTopology} using {@link TopologyTestDriver}.
 *
 * <h2>What is TopologyTestDriver?</h2>
 * <p>An in-memory Kafka Streams simulator provided by the
 * {@code kafka-streams-test-utils} library. It runs your real topology code
 * but replaces the Kafka broker with in-memory buffers. This means:
 * <ul>
 *   <li>No Docker or real Kafka needed — tests run in milliseconds</li>
 *   <li>You control wall-clock time (important for windowed test scenarios)</li>
 *   <li>State stores are directly queryable from the test</li>
 *   <li>100% of your topology logic is exercised — same code path as production</li>
 * </ul>
 *
 * <h2>Test structure</h2>
 * Each test follows Arrange-Act-Assert:
 * <ol>
 *   <li>Arrange: create the driver, input/output topics, and test records</li>
 *   <li>Act: pipe records into the input topic</li>
 *   <li>Assert: read from output topics OR query state stores directly</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ZMartTopologyTest {

    private TopologyTestDriver driver;

    // Input
    private TestInputTopic<String, Purchase> rawPurchaseInput;
    private TestInputTopic<String, Store>    storeInput;

    // Output
    private TestOutputTopic<String, Purchase>        maskedOutput;
    private TestOutputTopic<String, Purchase>        electronicsOutput;
    private TestOutputTopic<String, Purchase>        clothingOutput;
    private TestOutputTopic<String, Purchase>        enrichedOutput;
    private TestOutputTopic<String, PurchasePattern> patternOutput;
    private TestOutputTopic<String, RewardAccumulator> rewardsOutput;

    // ─────────────────────────────────────────────────────────────────────────
    // Setup / Teardown
    // ─────────────────────────────────────────────────────────────────────────

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = ZMartTopology.build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "zmart-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(topology, props);

        // Wire input topics
        rawPurchaseInput = driver.createInputTopic(
                ZMartTopics.PURCHASES_RAW,
                new StringSerializer(),
                new JsonSerializer<>()
        );
        storeInput = driver.createInputTopic(
                ZMartTopics.STORES,
                new StringSerializer(),
                new JsonSerializer<>()
        );

        // Wire output topics
        maskedOutput = driver.createOutputTopic(
                ZMartTopics.PURCHASES_MASKED,
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );
        electronicsOutput = driver.createOutputTopic(
                ZMartTopics.PURCHASES_ELECTRONICS,
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );
        clothingOutput = driver.createOutputTopic(
                ZMartTopics.PURCHASES_CLOTHING,
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );
        enrichedOutput = driver.createOutputTopic(
                ZMartTopics.PURCHASES_ENRICHED,
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );
        patternOutput = driver.createOutputTopic(
                ZMartTopics.PURCHASE_PATTERNS,
                new StringDeserializer(),
                new JsonDeserializer<>(PurchasePattern.class)
        );
        rewardsOutput = driver.createOutputTopic(
                ZMartTopics.REWARDS,
                new StringDeserializer(),
                new JsonDeserializer<>(RewardAccumulator.class)
        );
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Credit card masking
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void creditCardShouldBeMaskedBeforeReachingAnyOutputTopic() {
        rawPurchaseInput.pipeInput("key-1", buildPurchase("cust-001", "OTHER", 50.00,
                "4532-1234-5678-9101"));

        // The "other" department purchase goes to the masked topic
        Purchase result = maskedOutput.readValue();
        assertThat(result.getCreditCardNumber())
                .as("Credit card should be masked")
                .isEqualTo("XXXX-XXXX-XXXX-9101");

        assertThat(result.getCreditCardNumber())
                .doesNotContain("4532", "1234", "5678");
    }

    @Test
    @Order(2)
    void maskCreditCard_shouldHandleCardNumberWithoutDashes() {
        rawPurchaseInput.pipeInput("key-2", buildPurchase("cust-002", "OTHER", 25.00,
                "4532123456789101")); // no dashes

        Purchase result = maskedOutput.readValue();
        assertThat(result.getCreditCardNumber()).isEqualTo("XXXX-XXXX-XXXX-9101");
    }

    @Test
    @Order(3)
    void maskCreditCard_shouldHandleNullGracefully() {
        Purchase p = buildPurchase("cust-003", "OTHER", 10.00, null);
        rawPurchaseInput.pipeInput("key-3", p);

        Purchase result = maskedOutput.readValue();
        assertThat(result.getCreditCardNumber()).isNull(); // null passes through unchanged
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Department routing (branch)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(4)
    void electronicsPurchaseShouldGoToElectronicsTopic() {
        rawPurchaseInput.pipeInput("key-4", buildPurchase("cust-010", "ELECTRONICS",
                149.99, "1111-2222-3333-4444"));

        assertThat(electronicsOutput.readValue()).isNotNull();
        assertThat(maskedOutput.isEmpty()).isTrue();
        assertThat(clothingOutput.isEmpty()).isTrue();
    }

    @Test
    @Order(5)
    void clothingPurchaseShouldGoToClothingTopic() {
        rawPurchaseInput.pipeInput("key-5", buildPurchase("cust-011", "CLOTHING",
                79.99, "5555-6666-7777-8888"));

        assertThat(clothingOutput.readValue()).isNotNull();
        assertThat(maskedOutput.isEmpty()).isTrue();
        assertThat(electronicsOutput.isEmpty()).isTrue();
    }

    @Test
    @Order(6)
    void foodPurchaseShouldGoToDefaultMaskedTopic() {
        rawPurchaseInput.pipeInput("key-6", buildPurchase("cust-012", "FOOD",
                12.50, "9999-8888-7777-6666"));

        assertThat(maskedOutput.readValue()).isNotNull();
        assertThat(electronicsOutput.isEmpty()).isTrue();
        assertThat(clothingOutput.isEmpty()).isTrue();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Filter
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(7)
    void zeroDollarPurchaseShouldBeFilteredOut() {
        rawPurchaseInput.pipeInput("key-7", buildPurchase("cust-020", "FOOD",
                0.00, "1234-5678-9012-3456"));

        assertThat(maskedOutput.isEmpty()).as("Zero-value purchase should be dropped").isTrue();
        assertThat(patternOutput.isEmpty()).isTrue();
        assertThat(rewardsOutput.isEmpty()).isTrue();
    }

    @Test
    @Order(8)
    void purchaseWithNullCustomerIdShouldBeFilteredOut() {
        Purchase p = Purchase.builder()
                .purchaseId("txn-null-customer")
                .customerId(null)          // invalid — no customer ID
                .department("FOOD")
                .purchaseTotal(20.00)
                .creditCardNumber("1111-1111-1111-1111")
                .purchaseDate(Instant.now())
                .build();
        rawPurchaseInput.pipeInput("key-8", p);

        assertThat(maskedOutput.isEmpty()).isTrue();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Purchase patterns
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(9)
    void purchasePatternShouldContainCorrectFields() {
        Purchase p = Purchase.builder()
                .purchaseId("txn-301")
                .customerId("cust-030")
                .department("ELECTRONICS")
                .itemPurchased("Mechanical Keyboard")
                .quantity(1)
                .purchaseTotal(89.00)
                .creditCardNumber("1234-5678-9012-3456")
                .zipCode("10001")
                .purchaseDate(Instant.parse("2026-03-15T10:00:00Z"))
                .build();

        rawPurchaseInput.pipeInput("key-9", p);

        PurchasePattern pattern = patternOutput.readValue();
        assertThat(pattern).isNotNull();
        assertThat(pattern.getZipCode()).isEqualTo("10001");
        assertThat(pattern.getItemPurchased()).isEqualTo("Mechanical Keyboard");
        assertThat(pattern.getPurchaseTotal()).isEqualTo(89.00);
        assertThat(pattern.getDepartment()).isEqualTo("ELECTRONICS");

        // Credit card should NOT be in a PurchasePattern
        // (it's a different type — no CC field at all)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Rewards accumulation (stateful)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(10)
    void rewardsShouldAccumulateAcrossMultiplePurchasesForSameCustomer() {
        String customerId = "cust-100";

        rawPurchaseInput.pipeInput("key-10a", buildPurchase(customerId, "FOOD",     100.00, "1111-2222-3333-4444"));
        rawPurchaseInput.pipeInput("key-10b", buildPurchase(customerId, "CLOTHING",  50.00, "1111-2222-3333-4444"));
        rawPurchaseInput.pipeInput("key-10c", buildPurchase(customerId, "ELECTRONICS", 25.00, "1111-2222-3333-4444"));

        // 3 reward updates emitted (one per purchase)
        List<KeyValue<String, RewardAccumulator>> updates = rewardsOutput.readKeyValuesToList();
        assertThat(updates).hasSize(3);

        // The LAST update is the running total after all 3 purchases
        RewardAccumulator final_ = updates.get(2).value;
        assertThat(final_.getCustomerId()).isEqualTo(customerId);
        assertThat(final_.getCurrentPurchaseCount()).isEqualTo(3);
        assertThat(final_.getTotalPurchases()).isEqualTo(175.00);
        // Points = ceil(100*100) + ceil(50*100) + ceil(25*100) = 10000+5000+2500 = 17500
        assertThat(final_.getRewardPoints()).isEqualTo(17500L);
    }

    @Test
    @Order(11)
    void rewardsShouldBeIndependentBetweenDifferentCustomers() {
        rawPurchaseInput.pipeInput("kA", buildPurchase("cust-200", "FOOD", 100.00, "1111-0000-0000-1111"));
        rawPurchaseInput.pipeInput("kB", buildPurchase("cust-201", "FOOD", 200.00, "2222-0000-0000-2222"));

        List<KeyValue<String, RewardAccumulator>> updates = rewardsOutput.readKeyValuesToList();
        assertThat(updates).hasSize(2);

        RewardAccumulator custA = updates.stream()
                .filter(kv -> "cust-200".equals(kv.key))
                .map(kv -> kv.value)
                .findFirst().orElseThrow();
        RewardAccumulator custB = updates.stream()
                .filter(kv -> "cust-201".equals(kv.key))
                .map(kv -> kv.value)
                .findFirst().orElseThrow();

        assertThat(custA.getRewardPoints()).isEqualTo(10000L);  // $100 * 100 = 10000
        assertThat(custB.getRewardPoints()).isEqualTo(20000L);  // $200 * 100 = 20000
    }

    @Test
    @Order(12)
    void rewardsStoreShouldBeQueryableDirectly() {
        // After processing, you can query the local RocksDB store directly —
        // this is what Kafka Streams' Interactive Queries feature enables in production
        rawPurchaseInput.pipeInput("kQ", buildPurchase("cust-300", "FOOD", 150.00, "3333-0000-0000-3333"));

        KeyValueStore<String, RewardAccumulator> store =
                driver.getKeyValueStore(ZMartTopology.REWARDS_STORE);

        RewardAccumulator acc = store.get("cust-300");
        assertThat(acc).isNotNull();
        assertThat(acc.getRewardPoints()).isEqualTo(15000L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: GlobalKTable join / store enrichment
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(13)
    void purchaseShouldBeEnrichedWithStoreCityAndRegionFromGlobalKTable() {
        // First: populate the GlobalKTable (seed the compacted store topic)
        Store nyStore = new Store();
        nyStore.setStoreId("store-NYC-05");
        nyStore.setCity("New York");
        nyStore.setRegion("EAST");
        storeInput.pipeInput("store-NYC-05", nyStore);

        // Then: send a purchase that references this store
        Purchase p = Purchase.builder()
                .purchaseId("txn-enrich-1")
                .customerId("cust-400")
                .storeId("store-NYC-05")
                .department("OTHER")
                .itemPurchased("Coffee")
                .purchaseTotal(5.50)
                .creditCardNumber("4444-5555-6666-7777")
                .purchaseDate(Instant.now())
                .build();
        rawPurchaseInput.pipeInput("key-13", p);

        // The enriched topic should have the store's city and region
        Purchase enriched = enrichedOutput.readValue();
        assertThat(enriched).isNotNull();
        assertThat(enriched.getStoreCity()).isEqualTo("New York");
        assertThat(enriched.getStoreRegion()).isEqualTo("EAST");

        // Credit card should still be masked in the enriched record
        assertThat(enriched.getCreditCardNumber()).isEqualTo("XXXX-XXXX-XXXX-7777");
    }

    @Test
    @Order(14)
    void purchaseForUnknownStoreShouldNotAppearInEnrichedTopic() {
        // No store record seeded — inner join means no match → no output
        Purchase p = buildPurchase("cust-500", "OTHER", 10.00, "1234-5678-9012-3456");
        rawPurchaseInput.pipeInput("key-14", p);

        // Inner join: no store match → enriched topic gets nothing for this record
        assertThat(enrichedOutput.isEmpty())
                .as("No store in GlobalKTable → inner join produces no output")
                .isTrue();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: maskCreditCard helper (unit test, no Kafka involved)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @Order(15)
    void maskCreditCardHelper_withDashes() {
        Purchase p = buildPurchase("c", "FOOD", 1.0, "4532-1234-5678-9101");
        Purchase masked = ZMartTopology.maskCreditCard(p);
        assertThat(masked.getCreditCardNumber()).isEqualTo("XXXX-XXXX-XXXX-9101");
    }

    @Test
    @Order(16)
    void maskCreditCardHelper_withoutDashes() {
        Purchase p = buildPurchase("c", "FOOD", 1.0, "4532123456789101");
        Purchase masked = ZMartTopology.maskCreditCard(p);
        assertThat(masked.getCreditCardNumber()).isEqualTo("XXXX-XXXX-XXXX-9101");
    }

    @Test
    @Order(17)
    void maskCreditCardHelper_nullCard_returnsUnchanged() {
        Purchase p = buildPurchase("c", "FOOD", 1.0, null);
        Purchase masked = ZMartTopology.maskCreditCard(p);
        assertThat(masked.getCreditCardNumber()).isNull();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Convenience factory for test Purchase records.
     * Only sets the minimum fields needed for the topology to process the record.
     */
    private static Purchase buildPurchase(String customerId,
                                          String department,
                                          double total,
                                          String creditCard) {
        return Purchase.builder()
                .purchaseId("txn-" + System.nanoTime())
                .customerId(customerId)
                .storeId("store-TEST-01")
                .department(department)
                .itemPurchased("Test Item")
                .quantity(1)
                .purchaseTotal(total)
                .creditCardNumber(creditCard)
                .zipCode("00000")
                .purchaseDate(Instant.now())
                .build();
    }
}
