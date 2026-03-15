package com.example.zmart.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * Accumulated rewards for a single customer.
 *
 * <p>This is the <em>state</em> object in the rewards stateful aggregation.
 * Kafka Streams stores one instance of this per {@code customerId} key in RocksDB.
 * Every time a new purchase arrives for that customer, {@link #update(Purchase)}
 * is called with the current accumulator and the new purchase, and the result
 * is stored back in RocksDB + written to the changelog topic.
 *
 * <p>Points formula: {@code ceil(purchaseTotal * 100)} per purchase.
 * e.g. $149.99 purchase → 14,999 points.
 *
 * <p>Topology position:
 * <pre>
 *   maskedStream
 *     .selectKey(customerId)
 *     .groupByKey()
 *     .aggregate(RewardAccumulator::empty, (k, purchase, acc) -> acc.update(purchase))
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RewardAccumulator {

    private String customerId;
    private double totalPurchases;          // cumulative $ amount spent
    private long   rewardPoints;            // cumulative points
    private int    currentPurchaseCount;    // how many purchases total

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant dateOfFirstPurchase;

    public RewardAccumulator() {}

    /**
     * Initializer — called once when the first record for a new key arrives.
     * Must return a neutral/empty state (not yet updated with any purchase).
     */
    public static RewardAccumulator empty() {
        return new RewardAccumulator();
    }

    /**
     * Aggregator — called for every subsequent record.
     * Mutates this accumulator in place and returns itself.
     *
     * <p>Mutating {@code this} and returning it is the idiomatic Kafka Streams
     * pattern. Kafka Streams will serialize/deserialize the returned value,
     * so you get a fresh copy from the store on the next call anyway.
     */
    public RewardAccumulator update(Purchase purchase) {
        if (this.customerId == null) {
            this.customerId = purchase.getCustomerId();
        }
        if (this.dateOfFirstPurchase == null) {
            this.dateOfFirstPurchase = purchase.getPurchaseDate();
        }

        this.totalPurchases      += purchase.getPurchaseTotal();
        this.currentPurchaseCount++;
        this.rewardPoints        += (long) Math.ceil(purchase.getPurchaseTotal() * 100);
        return this;
    }

    // Getters & setters

    public String  getCustomerId()            { return customerId; }
    public void    setCustomerId(String v)    { this.customerId = v; }

    public double  getTotalPurchases()        { return totalPurchases; }
    public void    setTotalPurchases(double v){ this.totalPurchases = v; }

    public long    getRewardPoints()          { return rewardPoints; }
    public void    setRewardPoints(long v)    { this.rewardPoints = v; }

    public int     getCurrentPurchaseCount()          { return currentPurchaseCount; }
    public void    setCurrentPurchaseCount(int v)     { this.currentPurchaseCount = v; }

    public Instant getDateOfFirstPurchase()           { return dateOfFirstPurchase; }
    public void    setDateOfFirstPurchase(Instant v)  { this.dateOfFirstPurchase = v; }

    @Override
    public String toString() {
        return "RewardAccumulator{" +
               "customerId='" + customerId + '\'' +
               ", purchases=" + currentPurchaseCount +
               ", total=$" + String.format("%.2f", totalPurchases) +
               ", points=" + rewardPoints +
               '}';
    }
}
