package lab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaConnectionTest {
    private static final Map<String, String> ENV = Map.of(
        "KAFKA_BOOTSTRAP", "kafka1:9093,kafka2:9093",
        "KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
        "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512",
        "KAFKA_JAAS", "ScramLoginModule required username=\"u\" password=\"p\";");

    @Test
    void mapsEveryVariableToItsKafkaOption() {
        var properties = KafkaConnection.properties(ENV);

        assertEquals(4, properties.size());
        assertEquals("kafka1:9093,kafka2:9093", properties.getProperty("bootstrap.servers"));
        assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
        assertEquals("SCRAM-SHA-512", properties.getProperty("sasl.mechanism"));
        assertEquals(ENV.get("KAFKA_JAAS"), properties.getProperty("sasl.jaas.config"));
    }

    @Test
    void keepsEmptyValues() {
        var env = new HashMap<>(ENV);
        env.put("KAFKA_JAAS", "");

        assertEquals("", KafkaConnection.properties(env).getProperty("sasl.jaas.config"));
    }

    @Test
    void failsOnMissingVariable() {
        var env = new HashMap<>(ENV);
        env.remove("KAFKA_BOOTSTRAP");

        var error = assertThrows(IllegalArgumentException.class, () -> KafkaConnection.properties(env));
        assertTrue(error.getMessage().contains("KAFKA_BOOTSTRAP"));
    }
}
