package lab;

import java.nio.ByteBuffer;
import org.apache.flink.table.functions.ScalarFunction;

/** Standard Kafka IntegerDeserializer wire format: signed big-endian 32-bit integer. */
public class IntegerKafkaKey extends ScalarFunction {
    public Integer eval(byte[] key) {
        if (key == null) return null;
        if (key.length != 4) throw new IllegalArgumentException("Expected a four-byte Kafka INTEGER key");
        return ByteBuffer.wrap(key).getInt();
    }
}
