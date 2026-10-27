package lab;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.utils.Utils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import lab.SocleEtJob.Event;

/** Kafka-compatible updating windows. No watermark-induced final-only emission. */
public class WindowJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final long HOUR = 3600000L, GRACE = 86400000L;
    static final String[] GROUP = {"INDICATEURID", "VOIEID", "GAREID", "DATE_OPERATION", "R_FREQUENCEID", "DATECOMPTABLE"};
    static final String[] FROM = {"R_COMPTEURID", "R_VOIEID", "R_GAREID", "DATE_OPERATION", "R_FREQUENCEID", "DATE_COMPTABLE"};
    static final String[] ABS = {"R_INDICATEURID", "R_VOIEID", "R_GAREID", "DATE_DEBUT", "DATE_FIN", "DATE_COMPTABLE"};
    static JsonNode read(Event e) throws Exception { return JSON.readTree(e.value); }
    static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String text(JsonNode n, String f) { return n.path(f).isNull() || n.path(f).isMissingNode() ? null : n.path(f).asText(); }
    static byte[] windowKey(byte[] base, long start, long end, boolean session) {
        ByteBuffer b = ByteBuffer.allocate(base.length + (session ? 16 : 8)).put(base);
        // Kafka SessionKeySchema stores end, then start. TimeWindow stores start only.
        if (session) b.putLong(end);
        return b.putLong(start).array();
    }
    static int partition(Event e) { return Utils.toPositive(Utils.murmur2(e.key)) % 6; }
    static Event prepare(Event e, boolean hourly) throws Exception {
        if (e.value == null) return e;
        JsonNode v = read(e);
        ObjectNode k = JSON.createObjectNode();
        if (hourly) {
            if (e.key == null || e.key.length != 4) return new Event(null, null, e.timestamp);
            k.put(GROUP[0], ByteBuffer.wrap(e.key).getInt());
            for (int i=1; i<GROUP.length; i++) k.set(GROUP[i], v.get(FROM[i]));
        } else {
            JsonNode f = v.path("FLOW");
            k.set("KEYSTATIONLANE", v.get("STATIONLANE"));
            k.set("KEYSTATION", f.get("STATION")); k.set("KEYLANE", f.get("LANE"));
        }
        for(JsonNode component:k) if(component==null || component.isNull()) return new Event(null,null,e.timestamp);
        return new Event(bytes(k.toString()), e.value, e.timestamp);
    }
    static DataStream<Event> source(StreamExecutionEnvironment env, String topic, String group) {
        return env.fromSource(KafkaSource.<Event>builder().setProperties(KafkaConnection.properties())
            .setTopics(topic).setGroupId(group).setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new SocleEtJob.ReadRecord()).build(), WatermarkStrategy.noWatermarks(), group).uid(group);
    }
    static void sink(DataStream<Event> stream, String topic, String uid) {
        int suffix=uid.equals("hourly-sink")?8:uid.equals("quality-sink")||uid.equals("count-sink")?16:0;
        stream.sinkTo(KafkaSink.<Event>builder().setKafkaProducerConfig(KafkaConnection.properties())
            .setRecordSerializer(new WindowRecord(topic,suffix)).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()).uid(uid).name(uid);
    }
    public static class WindowRecord implements KafkaRecordSerializationSchema<Event> {
        final String topic;final int suffix;
        public WindowRecord(String topic,int suffix){this.topic=topic;this.suffix=suffix;}
        @Override public ProducerRecord<byte[],byte[]> serialize(Event e,KafkaSinkContext context,Long timestamp){
            // Kafka's WindowedStreamPartitioner hashes the business key, without window bytes.
            byte[] base=Arrays.copyOf(e.key,e.key.length-suffix);
            int p=Utils.toPositive(Utils.murmur2(base))%6;
            return new ProducerRecord<>(topic,p,e.timestamp,e.key,e.value==null?null:bytes(e.value));
        }
    }

    public static class Hourly extends KeyedProcessFunction<Integer, Event, Event> {
        transient MapState<String,String> aggregates;
        transient ValueState<Long> time;
        @Override public void open(OpenContext c) {
            aggregates=getRuntimeContext().getMapState(new MapStateDescriptor<>("hourly-windows",String.class,String.class));
            time=getRuntimeContext().getState(new ValueStateDescriptor<>("partition-stream-time",Long.class));
        }
        @Override public void processElement(Event e, Context c, Collector<Event> out) throws Exception {
            long clock=Math.max(time.value()==null ? -1 : time.value(),e.timestamp); time.update(clock);
            long start=e.timestamp/HOUR*HOUR, end=start+HOUR;
            if(end<=clock-(GRACE-HOUR)) return;
            List<String> expired=new ArrayList<>();
            for(Map.Entry<String,String> a:aggregates.entries()) if(JSON.readTree(a.getValue()).path("end").asLong()<=clock-(GRACE-HOUR)) expired.add(a.getKey());
            for(String k:expired) aggregates.remove(k);
            String id=Base64.getEncoder().encodeToString(windowKey(e.key,start,end,false));
            JsonNode old=aggregates.contains(id)?JSON.readTree(aggregates.get(id)):null;
            JsonNode v=read(e), key=JSON.readTree(e.key);
            double sum=old==null?0:old.path("sum").asDouble();
            if(v.hasNonNull("VALEUR")) sum+=v.get("VALEUR").doubleValue();
            long ts=old==null?e.timestamp:Math.max(old.path("ts").asLong(),e.timestamp);
            ObjectNode state=JSON.createObjectNode().put("sum",sum).put("ts",ts).put("end",end);
            aggregates.put(id,state.toString());
            ObjectNode result=JSON.createObjectNode();
            for(int i=0;i<GROUP.length;i++) {
                JsonNode n=key.get(GROUP[i]);
                if(n==null || n.isNull()) result.putNull(ABS[i]);
                else if(i==3 || i==5) result.put(ABS[i],Math.abs(n.doubleValue()));
                else result.put(ABS[i],Math.abs(n.intValue()));
            }
            result.put("VALEUR",sum);
            out.collect(new Event(Base64.getDecoder().decode(id),result.toString(),ts));
        }
    }

    static boolean accepts(JsonNode v) {
        JsonNode f=v.path("FLOW"); String caller=text(f,"CALLERMODE");
        if(caller==null || caller.equals("TD")) return false;
        boolean tariff=false,post=false;
        for(JsonNode cl:f.path("CLASS")) {
            String type=text(cl,"CLASSTYPE");
            tariff|="TarifiedClass".equals(type);post|="PostDTO".equals(type);
        }
        // In this engine's REDUCE lambda, null members do not turn false OR null into a match.
        return tariff&&!post;
    }
    static boolean wrong(JsonNode v) {
        for(JsonNode cl:v.path("FLOW").path("CLASS")) if("Manual".equals(text(cl,"CLASSORIGIN"))||"FC".equals(text(cl,"CLASSORIGIN"))) return true;
        return false;
    }
    public static class Sessions extends KeyedProcessFunction<Integer, Event, Event> {
        final boolean quality;
        transient MapState<String,String> sessions;
        transient ValueState<Long> time;
        transient ValueState<Long> sequence;
        public Sessions(boolean quality) {this.quality=quality;}
        @Override public void open(OpenContext c) {
            sessions=getRuntimeContext().getMapState(new MapStateDescriptor<>("sessions",String.class,String.class));
            time=getRuntimeContext().getState(new ValueStateDescriptor<>("partition-stream-time",Long.class));
            sequence=getRuntimeContext().getState(new ValueStateDescriptor<>("latest-arrival-sequence",Long.class));
        }
        boolean visible(JsonNode s) {return !quality || (s.path("n").asLong()>=20 && s.path("wrong").asDouble()/s.path("n").asDouble()>0.3);}
        @Override public void processElement(Event e, Context c, Collector<Event> out) throws Exception {
            long clock=Math.max(time.value()==null?-1:time.value(),e.timestamp); time.update(clock);
            long seq=sequence.value()==null?0:sequence.value()+1; sequence.update(seq);
            JsonNode v=read(e);
            String key=Base64.getEncoder().encodeToString(e.key);
            // State indexed by serialized business key and interval, retained until gap+grace expires.
            List<Map.Entry<String,JsonNode>> matches=new ArrayList<>(); List<String> expired=new ArrayList<>();
            for(Map.Entry<String,String> en:sessions.entries()) {
                JsonNode s=JSON.readTree(en.getValue());
                if(s.path("end").asLong()<clock-GRACE) {expired.add(en.getKey());continue;}
                if(s.path("key").asText().equals(key) && s.path("start").asLong()<=e.timestamp+HOUR && s.path("end").asLong()>=e.timestamp-HOUR)
                    matches.add(new AbstractMap.SimpleEntry<>(en.getKey(),s));
            }
            for(String k:expired) sessions.remove(k);
            matches.sort(Comparator.comparingLong(a->a.getValue().path("start").asLong()));
            long start=e.timestamp,end=e.timestamp,n=1,bad=quality&&wrong(v)?1:0;
            String latest=quality?text(v.path("FLOW"),"CREATIONDATE"):null;
            long latestSeq=latest==null?-1:seq;
            HyperLogLog hll=new HyperLogLog(14);
            if(!quality && text(v,"STATIONLANE")!=null) hll.offer(text(v,"STATIONLANE"));
            for(Map.Entry<String,JsonNode> en:matches) {
                JsonNode s=en.getValue(); start=Math.min(start,s.path("start").asLong());end=Math.max(end,s.path("end").asLong());
                n+=s.path("n").asLong();bad+=s.path("wrong").asLong();
                if(quality && s.path("latestSeq").asLong(-1)>latestSeq) {latest=text(s,"latest");latestSeq=s.path("latestSeq").asLong();}
                if(!quality) hll.addAll(HyperLogLog.Builder.build(Base64.getDecoder().decode(s.path("hll").asText())));
            }
            if(end<clock-GRACE) return;
            for(Map.Entry<String,JsonNode> en:matches) {
                JsonNode s=en.getValue();
                // Kafka skips removal when the merged interval is still the new point window.
                if((start!=e.timestamp || end!=e.timestamp) && visible(s))
                    out.collect(new Event(windowKey(e.key,s.path("start").asLong(),s.path("end").asLong(),true),null,s.path("end").asLong()));
                sessions.remove(en.getKey());
            }
            ObjectNode s=JSON.createObjectNode().put("key",key).put("start",start).put("end",end).put("n",n).put("wrong",bad);
            s.put("latest",latest).put("latestSeq",latestSeq);
            if(!quality) s.put("hll",Base64.getEncoder().encodeToString(hll.getBytes()));
            sessions.put(key+":"+start+":"+end,s.toString());
            if(!visible(s)) return;
            ObjectNode result=JSON.createObjectNode();
            if(quality) {
                JsonNode k=JSON.readTree(e.key);
                result.set("STATION",k.get("KEYSTATION"));result.set("LANE",k.get("KEYLANE"));result.set("STATIONLANE",k.get("KEYSTATIONLANE"));
                result.put("CREATIONDATE",latest).put("NBCLASSIFWRONGORIGIN",bad).put("RATIO",bad/(double)n).put("NBPASSAGE",n).put("COUNTKEY",1);
            } else result.put("NBTOTALVOIE",hll.cardinality()).put("COUNTKEYVALUE",v.path("COUNTKEY").asInt());
            result.put("WINDOW_START",start).put("WINDOW_END",end);
            out.collect(new Event(windowKey(e.key,start,end,true),result.toString(),end));
        }
    }

    /** Two-sided temporal join: every matching update is an event, table tombstones are ignored by STREAM contracts. */
    public static class AlarmJoin extends KeyedCoProcessFunction<String,Event,Event,Event> {
        transient ListState<String> left,right; transient ValueState<Long> leftTime,rightTime;
        @Override public void open(OpenContext c) {
            left=getRuntimeContext().getListState(new ListStateDescriptor<>("left-events",String.class));
            right=getRuntimeContext().getListState(new ListStateDescriptor<>("right-events",String.class));
            leftTime=getRuntimeContext().getState(new ValueStateDescriptor<>("left-store-time",Long.class));
            rightTime=getRuntimeContext().getState(new ValueStateDescriptor<>("right-store-time",Long.class));
        }
        void process(Event e,boolean isLeft,Collector<Event> out) throws Exception {
            ValueState<Long> ownTime=isLeft?leftTime:rightTime,otherTime=isLeft?rightTime:leftTime;
            long clock=Math.max(ownTime.value()==null?-1:ownTime.value(),e.timestamp);ownTime.update(clock);
            long otherClock=otherTime.value()==null?-1:otherTime.value();
            ListState<String> own=isLeft?left:right,other=isLeft?right:left;
            List<String> retained=new ArrayList<>();
            for(String raw:own.get()) if(JSON.readTree(raw).path("timestamp").asLong()>clock-GRACE) retained.add(raw);
            if(e.timestamp>clock-GRACE) retained.add(JSON.createObjectNode().put("timestamp",e.timestamp).put("value",e.value).toString());own.update(retained);
            List<JsonNode> ordered=new ArrayList<>();
            for(String raw:other.get()) ordered.add(JSON.readTree(raw));
            ordered.sort(Comparator.comparingLong(a->a.path("timestamp").asLong()));
            retained=new ArrayList<>();
            for(JsonNode row:ordered) {
                long ts=row.path("timestamp").asLong();
                if(ts<=otherClock-GRACE) continue;
                retained.add(row.toString());
                if(Math.abs(ts-e.timestamp)>60000) continue;
                JsonNode l=isLeft?read(e):JSON.readTree(row.path("value").asText());
                JsonNode r=isLeft?JSON.readTree(row.path("value").asText()):read(e);
                ObjectNode joined=JSON.createObjectNode();joined.set("left",l);joined.set("right",r);
                out.collect(new Event(null,joined.toString(),Math.max(ts,e.timestamp)));
            }
            other.update(retained);
        }
        @Override public void processElement1(Event e,Context c,Collector<Event> out)throws Exception{process(e,true,out);}
        @Override public void processElement2(Event e,Context c,Collector<Event> out)throws Exception{process(e,false,out);}
    }
    static String sub(String value,int begin,int end) {return value==null?null:value.substring(Math.min(begin,value.length()),Math.min(end,value.length()));}
    static String alarmKey(Event e,boolean global)throws Exception {
        JsonNode l=read(e).get("left");
        if(global) return JSON.writeValueAsString(sub(text(l,"STATION"),0,5));
        ObjectNode key=JSON.createObjectNode(); key.set("GARE",l.get("STATION"));key.set("VOIE",l.get("LANE"));return key.toString();
    }
    public static class AlarmAggregate extends KeyedProcessFunction<String,Event,Event> {
        final boolean global; transient ValueState<String> previous; transient ValueState<Long> time; transient ValueState<Boolean> visible;
        public AlarmAggregate(boolean global){this.global=global;}
        @Override public void open(OpenContext c){
            previous=getRuntimeContext().getState(new ValueStateDescriptor<>("alarm-values",String.class));
            time=getRuntimeContext().getState(new ValueStateDescriptor<>("alarm-timestamp",Long.class));
            visible=getRuntimeContext().getState(new ValueStateDescriptor<>("alarm-visible",Boolean.class));
        }
        @Override public void processElement(Event e,Context c,Collector<Event> out)throws Exception {
            JsonNode j=read(e),l=j.get("left"),r=j.get("right");
            JsonNode old=previous.value()==null?null:JSON.readTree(previous.value());
            ObjectNode result=JSON.createObjectNode();
            result.put("IDSOURCEALARME",3).put("IDTYPEALARME",3);
            result.set("PRIORITE",r.hasNonNull("NBTOTALVOIE")?r.get("NBTOTALVOIE"):old==null?NullNode.instance:old.get("PRIORITE"));
            result.put("IDCLASSE",0).put("IDCRITICITE",1);
            result.put("IDENSEMBLEEQUIPEMENT",global?"0":"").put("IDEQUIPEMENT","01");
            if(global) result.put("IDGARE","");else result.set("IDGARE",l.get("STATION"));
            result.put("IDTYPEINCIDENT",0).put("LIBELLEDETAILINCIDENT","").put("LIBELLEENSEMBLEEQUIPEMENT",global?"Société":"Voie");
            result.put("LIBELLEEQUIPEMENT","Classification").put("LIBELLEINCIDENT","Classif: origine de la classe anormale ("+(global?"Soc":"Voie")+")");
            if(global) result.put("LIBELLEVOIE","");else result.set("LIBELLEVOIE",l.get("LANE"));
            result.put("IDTYPEVOIE","").put("NUMEROTRANSACTION",0).put("TYPEVOIEENTREESORTIE",0);
            result.put("CODESOCIETE",sub(text(l,"STATION"),3,5));
            // SOCIETE is the global GROUP BY key; only the unit query includes it in the value.
            if(!global) result.put("SOCIETE",sub(text(l,"STATION"),0,5));
            if(l.hasNonNull("NBCLASSIFWRONGORIGIN"))result.set("TOTAL",l.get("NBCLASSIFWRONGORIGIN"));
            else result.set("TOTAL",old==null?NullNode.instance:old.get("TOTAL"));
            String date=text(l,"CREATIONDATE");
            if(date!=null) result.put("HORODATEALARME",OffsetDateTime.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSX"))
                .atZoneSameInstant(ZoneId.of("Europe/Paris")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
            else result.set("HORODATEALARME",old==null?NullNode.instance:old.get("HORODATEALARME"));
            long ts=Math.max(time.value()==null?-1:time.value(),e.timestamp);time.update(ts);previous.update(result.toString());
            boolean show=result.hasNonNull("PRIORITE") && (global?result.path("PRIORITE").asInt()>=30:result.path("PRIORITE").asInt()<30);
            if(show || Boolean.TRUE.equals(visible.value())) out.collect(new Event(bytes(c.getCurrentKey()),show?result.toString():null,ts));
            visible.update(show);
        }
    }

    public static void main(String[] args)throws Exception {
        if(args.length!=2||!args[0].equals("--config"))throw new IllegalArgumentException("Expected --config file.json");
        JsonNode config=JSON.readTree(Files.readString(Path.of(args[1])));
        Set<String> stages=new HashSet<>();if(config.has("stages"))for(JsonNode n:config.get("stages"))stages.add(n.asText());
        else stages.addAll(Arrays.asList("hourly","quality","count","alarms"));
        String prefix=config.path("group_prefix").asText("windows-flink");
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();env.enableCheckpointing(10000);
        ProjectKsqlJson project=new ProjectKsqlJson();
        if(stages.contains("hourly")) {
            DataStream<Event> input=source(env,config.get("indicator_input").asText(),prefix+"-indicator-input")
                .map(e->new Event(e.key,project.eval(e.value,"lot78-indicator-input"),e.timestamp)).returns(Event.class)
                .filter(e->e.value!=null).map(e->prepare(e,true)).returns(Event.class).filter(e->e.value!=null);
            sink(input.keyBy(WindowJob::partition).process(new Hourly()).uid("hourly-aggregate"),config.get("hourly").asText(),"hourly-sink");
        }
        if(stages.contains("quality")) {
            DataStream<Event> input=source(env,config.get("flows").asText(),prefix+"-flows")
                .map(e->new Event(e.key,project.eval(e.value,"lot78-flows"),e.timestamp)).returns(Event.class)
                .filter(e->e.value!=null&&accepts(read(e))).map(e->prepare(e,false)).returns(Event.class).filter(e->e.value!=null);
            sink(input.keyBy(WindowJob::partition).process(new Sessions(true)).uid("quality-sessions"),config.get("quality").asText(),"quality-sink");
        }
        DataStream<Event> quality=null,count=null;
        if(stages.contains("count")||stages.contains("alarms")) quality=source(env,config.get("quality").asText(),prefix+"-quality")
            .map(e->new Event(e.key,project.eval(e.value,"lot78-quality-input"),e.timestamp)).returns(Event.class)
            .filter(e->e.value!=null&&read(e).hasNonNull("COUNTKEY"));
        if(stages.contains("count")) {
            DataStream<Event> rekeyed=quality.map(e->new Event(bytes(read(e).get("COUNTKEY").asText()),e.value,e.timestamp)).returns(Event.class);
            sink(rekeyed.keyBy(WindowJob::partition).process(new Sessions(false)).uid("count-sessions"),config.get("count").asText(),"count-sink");
        }
        if(stages.contains("alarms")) {
            count=source(env,config.get("count").asText(),prefix+"-count")
                .map(e->new Event(e.key,project.eval(e.value,"lot78-count-input"),e.timestamp)).returns(Event.class)
                .filter(e->e.value!=null&&read(e).hasNonNull("COUNTKEYVALUE"));
            DataStream<Event> joined=quality.keyBy(e->read(e).get("COUNTKEY").asText())
                .connect(count.keyBy(e->read(e).get("COUNTKEYVALUE").asText())).process(new AlarmJoin()).uid("alarm-temporal-join");
            for(boolean global:new boolean[]{false,true}) {
                String name=global?"global_alarm":"unit_alarm";
                sink(joined.filter(e->{JsonNode l=read(e).get("left");return l.hasNonNull("STATION")&&(global||l.hasNonNull("LANE"));})
                    .keyBy(e->alarmKey(e,global)).process(new AlarmAggregate(global)).uid(name+"-aggregate"),config.get(name).asText(),name+"-sink");
            }
        }
        env.execute("Lots 7/8 - updating Kafka windows and classification alarms");
    }
}
