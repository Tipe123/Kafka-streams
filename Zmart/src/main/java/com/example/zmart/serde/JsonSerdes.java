package com.example.zmart.serde;

import com.example.zmart.model.Purchase;
import com.example.zmart.model.PurchasePattern;
import com.example.zmart.model.RewardAccumulator;
import com.example.zmart.model.Store;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Factory for all ZMart domain-object SerDes (Serializer + Deserializer pairs).
 *
 * <p>A {@link Serde} bundles a Serializer and Deserializer together so you can
 * pass a single object to {@code Consumed.with(...)}, {@code Produced.with(...)},
 * and {@code Materialized.with(...)}.
 *
 * <p>Usage:
 * <pre>{@code
 *   KStream<String, Purchase> stream = builder.stream(
 *       "zmart.purchases.raw",
 *       Consumed.with(Serdes.String(), JsonSerdes.purchase())
 *   );
 * }</pre>
 */
public final class JsonSerdes {

    private JsonSerdes() {}  // utility class — no instances

    /** Serde for {@link Purchase}. */
    public static Serde<Purchase> purchase() {
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(Purchase.class));
    }

    /** Serde for {@link PurchasePattern}. */
    public static Serde<PurchasePattern> purchasePattern() {
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(PurchasePattern.class));
    }

    /** Serde for {@link RewardAccumulator}. */
    public static Serde<RewardAccumulator> rewardAccumulator() {
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(RewardAccumulator.class));
    }

    /** Serde for {@link Store}. */
    public static Serde<Store> store() {
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(Store.class));
    }
}
