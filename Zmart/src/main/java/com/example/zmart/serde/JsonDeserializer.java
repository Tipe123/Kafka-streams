package com.example.zmart.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Jackson-based Kafka Deserializer.
 *
 * <p>Converts a UTF-8 JSON byte array back to a typed Java object.
 *
 * <p>Usage in a topology:
 * <pre>{@code
 *   Consumed.with(Serdes.String(), new JsonSerdes<>(Purchase.class))
 * }</pre>
 *
 * <p><strong>Error handling note:</strong> If deserialization fails (bad/corrupt JSON),
 * this throws a {@link RuntimeException}. Combined with the
 * {@code LogAndContinueExceptionHandler} in {@link com.example.zmart.ZMartApp},
 * bad records are logged and skipped — they do not crash the application.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonDeserializer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Class<T> targetType;

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            return MAPPER.readValue(data, targetType);
        } catch (Exception e) {
            log.error("Failed to deserialize record on topic {}: {}", topic, e.getMessage(), e);
            // Throwing here lets DeserializationExceptionHandler decide to skip or fail
            throw new RuntimeException("Deserialization failed for type " + targetType.getSimpleName(), e);
        }
    }
}
