package lab;

import java.util.Map;
import java.util.Properties;

/** Kafka client connection settings, read from the same variables as the Flink SQL scripts. */
public final class KafkaConnection {
    private static final Map<String, String> VARIABLES = Map.of(
        "bootstrap.servers", "KAFKA_BOOTSTRAP",
        "security.protocol", "KAFKA_SECURITY_PROTOCOL",
        "sasl.mechanism", "KAFKA_SASL_MECHANISM",
        "sasl.jaas.config", "KAFKA_JAAS");

    private KafkaConnection() {
    }

    public static Properties properties() {
        return properties(System.getenv());
    }

    static Properties properties(Map<String, String> env) {
        var properties = new Properties();
        VARIABLES.forEach((option, variable) -> {
            var value = env.get(variable);
            // No default: a missing variable must not silently target another broker.
            if (value == null) {
                throw new IllegalArgumentException("Undefined variable: " + variable);
            }
            properties.setProperty(option, value);
        });
        return properties;
    }
}
