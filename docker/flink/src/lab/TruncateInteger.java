package lab;
import org.apache.flink.table.functions.ScalarFunction;

/** ksqlDB DOUBLE-to-INTEGER truncation, including null propagation. */
public class TruncateInteger extends ScalarFunction {
    public Integer eval(Double value) {
        return value == null ? null : value.intValue();
    }
}
