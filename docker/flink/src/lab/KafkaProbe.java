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
        if (args[0].equals("barriers")) {
            JsonNode specs = mapper.readTree(Files.readString(Path.of(args[1])));
            try (org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient admin =
                    org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient.create(config)) {
                java.util.List<String> groups = new java.util.ArrayList<>();
                admin.listConsumerGroups().all().get(10, java.util.concurrent.TimeUnit.SECONDS)
                    .forEach(g -> groups.add(g.groupId()));
                ObjectNode result = mapper.createObjectNode();
                java.util.Iterator<java.util.Map.Entry<String, JsonNode>> entries = specs.fields();
                while (entries.hasNext()) {
                    java.util.Map.Entry<String, JsonNode> entry = entries.next();
                    JsonNode spec = entry.getValue();
                    String group = spec.has("group") ? spec.get("group").asText() :
                        groups.stream().filter(g -> g.contains(spec.get("contains").asText())).findFirst().orElse(null);
                    ObjectNode status = mapper.createObjectNode();
                    status.put("ready", false);
                    if (group != null && groups.contains(group)) {
                        String topic = spec.get("topic").asText();
                        java.util.Map<org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition,
                            org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                            admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, java.util.concurrent.TimeUnit.SECONDS);
                        java.util.Map<org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition,
                            org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.OffsetSpec> latest = new java.util.HashMap<>();
                        committed.keySet().forEach(p -> latest.put(p,
                            org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.OffsetSpec.latest()));
                        admin.describeTopics(Collections.singleton(topic)).allTopicNames().get(10, java.util.concurrent.TimeUnit.SECONDS)
                            .get(topic).partitions().forEach(p -> latest.put(
                                new org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition(topic, p.partition()),
                                org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.OffsetSpec.latest()));
                        java.util.Map<org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition,
                            org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> ends =
                            admin.listOffsets(latest).all().get(10, java.util.concurrent.TimeUnit.SECONDS);
                        long lag = 0;
                        for (var end : ends.entrySet()) {
                            var offset = committed.get(end.getKey());
                            // An empty partition needs no committed offset: no record is pending.
                            lag += offset == null ? end.getValue().offset() : Math.max(0, end.getValue().offset() - offset.offset());
                        }
                        status.put("group", group); status.put("lag", lag); status.put("ready", lag == 0);
                    }
                    result.set(entry.getKey(), status);
                }
                System.out.println(mapper.writeValueAsString(result));
            }
        } else if (args[0].equals("produce") || args[0].equals("produce-many")) {
            config.put("key.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put("value.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put("acks", "all");
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
                for (String line : Files.readAllLines(Path.of(args[2]))) {
                    JsonNode row = mapper.readTree(line);
                    byte[] key = row.get("key").isNull() ? null : Base64.getDecoder().decode(row.get("key").asText());
                    byte[] value = row.get("value").isNull() ? null : mapper.writeValueAsBytes(row.get("value"));
                    String topic = args[0].equals("produce-many") ? row.get("topic").asText() : args[1];
                    Integer partition = row.get("partition").isNull() ? null : row.get("partition").asInt();
                    producer.send(new ProducerRecord<>(topic, partition,
                        row.get("timestamp").asLong(), key, value)).get();
                }
            }
        } else if (args[0].equals("consume") || args[0].equals("consume-many")) {
            config.put("key.deserializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer");
            config.put("value.deserializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer");
            config.put("group.id", "probe-" + java.util.UUID.randomUUID());
            config.put("auto.offset.reset", "earliest");
            config.put("enable.auto.commit", "false");
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
                consumer.subscribe(java.util.Arrays.asList(args[1].split(",")));
                long deadline = System.currentTimeMillis() + 45000;
                long lastRecord = 0;
                while (System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                        ObjectNode row = mapper.createObjectNode();
                        if (args[0].equals("consume-many")) row.put("topic", record.topic());
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
