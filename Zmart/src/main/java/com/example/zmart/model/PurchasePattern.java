package com.example.zmart.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * A lightweight projection of a Purchase, keyed by ZIP code.
 *
 * <p>This is a <em>stateless</em> transformation of {@link Purchase} — no memory
 * between records is needed. It is used by analytics consumers to understand
 * what is being bought where, without exposing PII or payment data.
 *
 * <p>Topology position: rawStream → mapValues(PurchasePattern::from)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurchasePattern {

    private String  zipCode;
    private String  itemPurchased;
    private double  purchaseTotal;
    private String  department;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant purchaseDate;

    public PurchasePattern() {}

    /**
     * Static factory — creates a PurchasePattern from a (masked) Purchase.
     * This is the function passed to {@code mapValues} in the topology.
     */
    public static PurchasePattern from(Purchase purchase) {
        PurchasePattern pp = new PurchasePattern();
        pp.zipCode       = purchase.getZipCode();
        pp.itemPurchased = purchase.getItemPurchased();
        pp.purchaseTotal = purchase.getPurchaseTotal();
        pp.department    = purchase.getDepartment();
        pp.purchaseDate  = purchase.getPurchaseDate();
        return pp;
    }

    // Getters & setters

    public String  getZipCode()        { return zipCode; }
    public void    setZipCode(String v)    { this.zipCode = v; }

    public String  getItemPurchased()  { return itemPurchased; }
    public void    setItemPurchased(String v) { this.itemPurchased = v; }

    public double  getPurchaseTotal()  { return purchaseTotal; }
    public void    setPurchaseTotal(double v) { this.purchaseTotal = v; }

    public String  getDepartment()     { return department; }
    public void    setDepartment(String v) { this.department = v; }

    public Instant getPurchaseDate()   { return purchaseDate; }
    public void    setPurchaseDate(Instant v) { this.purchaseDate = v; }

    @Override
    public String toString() {
        return "PurchasePattern{zip='" + zipCode +
               "', item='" + itemPurchased +
               "', total=" + purchaseTotal + '}';
    }
}
