package lab;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Test transport: exact key bytes, explicit producer timestamps and JSON record values. */
public class KafkaProbe {
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Properties config = new Properties();
        config.put("bootstrap.servers", "kafka:9092");
        if (args[0].equals("produce")) {
            config.put("key.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put("value.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put("acks", "all");
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
                for (String line : Files.readAllLines(Path.of(args[2]))) {
                    JsonNode row = mapper.readTree(line);
                    byte[] key = row.get("key").isNull() ? null : Base64.getDecoder().decode(row.get("key").asText());
                    byte[] value = row.get("value").isNull() ? null : mapper.writeValueAsBytes(row.get("value"));
                    producer.send(new ProducerRecord<>(args[1], row.get("partition").asInt(),
                        row.get("timestamp").asLong(), key, value)).get();
                }
            }
        } else if (args[0].equals("consume")) {
            config.put("key.deserializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer");
            config.put("value.deserializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer");
            config.put("group.id", "probe-" + java.util.UUID.randomUUID());
            config.put("auto.offset.reset", "earliest");
            config.put("enable.auto.commit", "false");
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
                consumer.subscribe(Collections.singleton(args[1]));
                long deadline = System.currentTimeMillis() + 45000;
                long lastRecord = 0;
                while (System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                        ObjectNode row = mapper.createObjectNode();
                        if (record.key() == null) row.putNull("key");
                        else row.put("key", Base64.getEncoder().encodeToString(record.key()));
                        row.put("timestamp", record.timestamp());
                        row.set("value", record.value() == null ? mapper.nullNode() : mapper.readTree(record.value()));
                        System.out.println(mapper.writeValueAsString(row));
                        lastRecord = System.currentTimeMillis();
                    }
                    if (lastRecord != 0 && System.currentTimeMillis() - lastRecord > 5000) break;
                }
            }
        } else throw new IllegalArgumentException("Expected produce or consume");
    }
}
