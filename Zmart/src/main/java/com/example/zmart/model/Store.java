package com.example.zmart.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Reference data for a ZMart store location.
 *
 * <p>This is loaded into a {@code GlobalKTable} from the compacted topic
 * {@code zmart.stores}. Because it is a GlobalKTable (not a partitioned KTable),
 * <em>every</em> Kafka Streams instance has the complete dataset in its local
 * RocksDB, enabling foreign-key joins without repartitioning.
 *
 * <p>The Kafka message key for this record is {@code storeId}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Store {

    private String storeId;
    private String storeName;
    private String city;
    private String region;      // e.g. "EAST", "WEST", "SOUTH"
    private String country;
    private String address;
    private int    employeeCount;

    public Store() {}

    // Getters & setters

    public String getStoreId()           { return storeId; }
    public void   setStoreId(String v)   { this.storeId = v; }

    public String getStoreName()         { return storeName; }
    public void   setStoreName(String v) { this.storeName = v; }

    public String getCity()              { return city; }
    public void   setCity(String v)      { this.city = v; }

    public String getRegion()            { return region; }
    public void   setRegion(String v)    { this.region = v; }

    public String getCountry()           { return country; }
    public void   setCountry(String v)   { this.country = v; }

    public String getAddress()           { return address; }
    public void   setAddress(String v)   { this.address = v; }

    public int    getEmployeeCount()           { return employeeCount; }
    public void   setEmployeeCount(int v)      { this.employeeCount = v; }

    @Override
    public String toString() {
        return "Store{id='" + storeId + "', city='" + city + "', region='" + region + "'}";
    }
}
