package com.example.zmart.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * Represents a single retail purchase at a ZMart store.
 *
 * <p>This is the central domain event. Every downstream topic, aggregation, and
 * join is ultimately derived from this record.
 *
 * <p>Key design decisions:
 * <ul>
 *   <li>The Kafka message key will be {@code customerId} (after selectKey), so that
 *       all purchases by the same customer land on the same partition for grouping.
 *   <li>{@code creditCardNumber} starts as the raw number and is masked to
 *       "XXXX-XXXX-XXXX-{last4}" in the first topology step.
 *   <li>All monetary values are {@code double}. In production, prefer
 *       {@code java.math.BigDecimal} to avoid floating-point drift.
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Purchase {

    // --- identifiers ---------------------------------------------------------
    private String purchaseId;     // UUID of this transaction
    private String customerId;     // loyalty / account ID
    private String storeId;        // e.g. "store-NYC-05"
    private String employeeId;     // cashier / POS terminal

    // --- purchase details ----------------------------------------------------
    private String department;     // ELECTRONICS, CLOTHING, FOOD, OTHER
    private String itemPurchased;
    private int    quantity;
    private double price;          // unit price
    private double purchaseTotal;  // quantity * price (before tax)

    // --- payment & location --------------------------------------------------
    private String  creditCardNumber; // masked downstream: XXXX-XXXX-XXXX-9101
    private String  zipCode;

    // --- store enrichment (filled by join with StoreGlobalKTable) ------------
    private String storeRegion;   // e.g. "EAST", "WEST"
    private String storeCity;     // e.g. "New York"

    // --- temporal ------------------------------------------------------------
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant purchaseDate;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public Purchase() {}

    // Copy constructor — useful in mapValues to create a mutated copy
    public Purchase(Purchase other) {
        this.purchaseId      = other.purchaseId;
        this.customerId      = other.customerId;
        this.storeId         = other.storeId;
        this.employeeId      = other.employeeId;
        this.department      = other.department;
        this.itemPurchased   = other.itemPurchased;
        this.quantity        = other.quantity;
        this.price           = other.price;
        this.purchaseTotal   = other.purchaseTotal;
        this.creditCardNumber = other.creditCardNumber;
        this.zipCode         = other.zipCode;
        this.storeRegion     = other.storeRegion;
        this.storeCity       = other.storeCity;
        this.purchaseDate    = other.purchaseDate;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder() { return new Builder(); }

    /** Copy-then-modify builder — preserves all fields by default. */
    public static Builder builder(Purchase base) { return new Builder(base); }

    public static final class Builder {
        private final Purchase p;

        private Builder()           { p = new Purchase(); }
        private Builder(Purchase b) { p = new Purchase(b); }

        public Builder purchaseId(String v)       { p.purchaseId = v;      return this; }
        public Builder customerId(String v)       { p.customerId = v;      return this; }
        public Builder storeId(String v)          { p.storeId = v;         return this; }
        public Builder employeeId(String v)       { p.employeeId = v;      return this; }
        public Builder department(String v)       { p.department = v;      return this; }
        public Builder itemPurchased(String v)    { p.itemPurchased = v;   return this; }
        public Builder quantity(int v)            { p.quantity = v;        return this; }
        public Builder price(double v)            { p.price = v;           return this; }
        public Builder purchaseTotal(double v)    { p.purchaseTotal = v;   return this; }
        public Builder creditCardNumber(String v) { p.creditCardNumber = v; return this; }
        public Builder zipCode(String v)          { p.zipCode = v;         return this; }
        public Builder storeRegion(String v)      { p.storeRegion = v;     return this; }
        public Builder storeCity(String v)        { p.storeCity = v;       return this; }
        public Builder purchaseDate(Instant v)    { p.purchaseDate = v;    return this; }

        public Purchase build() { return p; }
    }

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public String getPurchaseId()      { return purchaseId; }
    public void   setPurchaseId(String v) { this.purchaseId = v; }

    public String getCustomerId()      { return customerId; }
    public void   setCustomerId(String v) { this.customerId = v; }

    public String getStoreId()         { return storeId; }
    public void   setStoreId(String v) { this.storeId = v; }

    public String getEmployeeId()      { return employeeId; }
    public void   setEmployeeId(String v) { this.employeeId = v; }

    public String getDepartment()      { return department; }
    public void   setDepartment(String v) { this.department = v; }

    public String getItemPurchased()   { return itemPurchased; }
    public void   setItemPurchased(String v) { this.itemPurchased = v; }

    public int    getQuantity()        { return quantity; }
    public void   setQuantity(int v)   { this.quantity = v; }

    public double getPrice()           { return price; }
    public void   setPrice(double v)   { this.price = v; }

    public double getPurchaseTotal()   { return purchaseTotal; }
    public void   setPurchaseTotal(double v) { this.purchaseTotal = v; }

    public String getCreditCardNumber()      { return creditCardNumber; }
    public void   setCreditCardNumber(String v) { this.creditCardNumber = v; }

    public String getZipCode()         { return zipCode; }
    public void   setZipCode(String v) { this.zipCode = v; }

    public String getStoreRegion()     { return storeRegion; }
    public void   setStoreRegion(String v) { this.storeRegion = v; }

    public String getStoreCity()       { return storeCity; }
    public void   setStoreCity(String v) { this.storeCity = v; }

    public Instant getPurchaseDate()   { return purchaseDate; }
    public void    setPurchaseDate(Instant v) { this.purchaseDate = v; }

    @Override
    public String toString() {
        return "Purchase{" +
               "purchaseId='" + purchaseId + '\'' +
               ", customerId='" + customerId + '\'' +
               ", department='" + department + '\'' +
               ", item='" + itemPurchased + '\'' +
               ", total=" + purchaseTotal +
               ", cc='" + creditCardNumber + '\'' +
               '}';
    }
}
