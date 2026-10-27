package lab;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Lot 6: non-retroactive stream/table joins and arrival-ordered reference aggregates. */
public class SocleEtJob {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static class Event {
        public byte[] key;
        public String value;
        public long timestamp;
        public Event() {}
        public Event(byte[] key, String value, long timestamp) {
            this.key = key; this.value = value; this.timestamp = timestamp;
        }
    }

    public static class ReadRecord implements KafkaRecordDeserializationSchema<Event> {
        @Override public TypeInformation<Event> getProducedType() { return TypeInformation.of(Event.class); }
        @Override public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) {
            out.collect(new Event(record.key(), record.value() == null ? null :
                new String(record.value(), java.nio.charset.StandardCharsets.UTF_8), record.timestamp()));
        }
    }

    public static class WriteRecord implements KafkaRecordSerializationSchema<Event> {
        private final String topic;
        public WriteRecord(String topic) { this.topic = topic; }
        @Override public ProducerRecord<byte[], byte[]> serialize(Event event, KafkaSinkContext context, Long timestamp) {
            return new ProducerRecord<>(topic, null, event.timestamp, event.key,
                event.value == null ? null : event.value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private static DataStream<Event> source(StreamExecutionEnvironment env, String topic, String group) {
        return env.fromSource(KafkaSource.<Event>builder().setProperties(KafkaConnection.properties())
            .setTopics(topic).setGroupId(group).setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new ReadRecord()).build(), WatermarkStrategy.noWatermarks(), group).uid(group);
    }

    private static void sink(DataStream<Event> stream, String topic, String name) {
        stream.sinkTo(KafkaSink.<Event>builder().setKafkaProducerConfig(KafkaConnection.properties())
            .setRecordSerializer(new WriteRecord(topic)).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()).name(name).uid(name);
    }

    private static JsonNode read(Event event) throws Exception { return JSON.readTree(event.value); }
    private static boolean present(JsonNode value, String name) { return value.hasNonNull(name); }
    private static byte[] key(int id) { return ByteBuffer.allocate(4).putInt(id).array(); }
    private static int keyId(Event event) {
        if (event.key == null || event.key.length != 4) throw new IllegalArgumentException("Invalid reference INTEGER key");
        return ByteBuffer.wrap(event.key).getInt();
    }
    private static String fieldId(Event event, String name) throws Exception {
        return Integer.toString(read(event).get(name).intValue());
    }

    public static class AggregateReference extends KeyedProcessFunction<String, Event, Event> {
        private final String[] fields;
        private transient ValueState<Long> count;
        private transient ValueState<Long> maxTimestamp;
        public AggregateReference(String[] fields) { this.fields = fields; }
        @Override public void open(OpenContext context) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("accepted-count", Long.class));
            maxTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("aggregate-timestamp", Long.class));
        }
        @Override public void processElement(Event event, Context context, Collector<Event> out) throws Exception {
            JsonNode value = read(event);
            long next = count.value() == null ? 1 : count.value() + 1;
            long timestamp = maxTimestamp.value() == null ? event.timestamp : Math.max(maxTimestamp.value(), event.timestamp);
            ObjectNode result = JSON.createObjectNode();
            result.put("COUNT", next);
            // The WHERE clause requires all fields non-null; latest accepted arrival is LATEST_BY_OFFSET.
            for (String field : fields) result.set(field, value.get(field));
            count.update(next); maxTimestamp.update(timestamp);
            out.collect(new Event(key(value.get("ID").intValue()), JSON.writeValueAsString(result), timestamp));
        }
    }

    public static class Enrich extends KeyedCoProcessFunction<String, Event, Event, Event> {
        private final boolean compteur;
        private transient ValueState<String> reference;
        public Enrich(boolean compteur) { this.compteur = compteur; }
        @Override public void open(OpenContext context) {
            reference = getRuntimeContext().getState(new ValueStateDescriptor<>("current-reference", String.class));
        }
        @Override public void processElement2(Event event, Context context, Collector<Event> out) throws Exception {
            // Reference-only updates and deletes never re-emit past facts.
            if (event.value == null) reference.clear(); else reference.update(event.value);
        }
        private boolean same(JsonNode left, JsonNode right, String field) {
            return present(left, field) && present(right, field) && left.get(field).intValue() == right.get(field).intValue();
        }
        @Override public void processElement1(Event event, Context context, Collector<Event> out) throws Exception {
            if (reference.value() == null) return;
            JsonNode fact = read(event), ref = JSON.readTree(reference.value());
            if (compteur) {
                if (!same(fact, ref, "R_OPERATIONID") || !same(fact, ref, "R_TRANSACTION_ETATID")) return;
                if (Math.abs(ref.get("R_MODE_REGLEMENTID").intValue()) != 1 && !same(fact, ref, "R_MODE_REGLEMENTID")) return;
            }
            ObjectNode result = JSON.createObjectNode();
            String[] copy = {"R_VOIEID", "R_GAREID", "R_SOCIETEID", "R_PARTENAIREID", "IDT_TRAN_PEA"};
            for (String field : copy) result.set(field, fact.get(field));
            String type = compteur ? "R_COMPTEUR_TYPEID" : "R_INDICATEUR_TYPEID";
            if (Math.abs(ref.get(type).intValue()) == 1) result.put("VALEUR", 1.0);
            else result.set("VALEUR", fact.get("MONTANT_TRANSACTION"));
            if (compteur) result.set("MONTANT_TRANSACTION", fact.get("MONTANT_TRANSACTION"));
            result.set("DATE_OPERATION", fact.get("DATE_OPERATION"));
            result.set("DATE_COMPTABLE", fact.get("DATE_COMPTABLE"));
            result.set("R_FREQUENCEID", ref.get("R_FREQUENCEID"));
            if (compteur) result.set("C_R_MODE_REGLEMENTID", ref.get("R_MODE_REGLEMENTID"));
            result.set(type, ref.get(type));
            out.collect(new Event(key(fact.get("R_COMPTEURID").intValue()), JSON.writeValueAsString(result), event.timestamp));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !args[0].equals("--config")) throw new IllegalArgumentException("Expected --config file.json");
        JsonNode config = JSON.readTree(Files.readString(Path.of(args[1])));
        String prefix = config.path("group_prefix").asText("socleet-flink");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        ProjectKsqlJson projection = new ProjectKsqlJson();
        DataStream<Event> facts = source(env, config.get("facts").asText(), prefix + "-facts")
            .map(e -> new Event(e.key, projection.eval(e.value, "lot6-facts"), e.timestamp))
            .returns(Event.class).filter(e -> e.value != null && present(read(e), "R_COMPTEURID"));
        for (boolean compteur : new boolean[]{true, false}) {
            String kind = compteur ? "compteur" : "indicateur";
            String[] fields = compteur ? new String[]{"R_COMPTEUR_TYPEID", "R_FREQUENCEID", "R_OPERATIONID", "R_MODE_REGLEMENTID", "R_TRANSACTION_ETATID"}
                : new String[]{"R_INDICATEUR_TYPEID", "R_FREQUENCEID"};
            DataStream<Event> accepted = source(env, config.get(kind + "_input").asText(), prefix + "-" + kind + "-input")
                .map(e -> new Event(e.key, projection.eval(e.value, "lot6-" + kind), e.timestamp)).returns(Event.class)
                .filter(e -> {
                    if (e.value == null) return false;
                    JsonNode value = read(e);
                    if (!present(value, "ID")) return false;
                    for (String field : fields) if (!present(value, field)) return false;
                    return true;
                });
            DataStream<Event> aggregated = accepted.keyBy(e -> fieldId(e, "ID"))
                .process(new AggregateReference(fields)).name(kind + " reference aggregate").uid(kind + "-aggregate");
            sink(aggregated, config.get(kind + "_table").asText(), kind + "-table-sink");
            DataStream<Event> refs = source(env, config.get(kind + "_table").asText(), prefix + "-" + kind + "-lookup")
                .filter(e -> e.key != null);
            DataStream<Event> enriched = facts.keyBy(e -> fieldId(e, "R_COMPTEURID"))
                .connect(refs.keyBy(e -> Integer.toString(keyId(e))))
                .process(new Enrich(compteur)).name(kind + " fact-triggered lookup").uid(kind + "-lookup");
            sink(enriched, config.get(kind + "_output").asText(), kind + "-output-sink");
        }
        env.execute("Socle ET - lot 6 reference aggregates and non-retroactive enrichment");
    }
}
