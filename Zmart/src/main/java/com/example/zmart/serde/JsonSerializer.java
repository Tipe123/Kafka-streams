package com.example.zmart.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Jackson-based Kafka Serializer.
 *
 * <p>Converts any Java object to a UTF-8 JSON byte array.
 *
 * <p>Usage in a topology:
 * <pre>{@code
 *   Produced.with(Serdes.String(), new JsonSerdes<>(Purchase.class))
 * }</pre>
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Failed to serialize record on topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Serialization failed", e);
        }
    }
}
