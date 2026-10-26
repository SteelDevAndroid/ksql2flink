package lab;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** JSON encoding for a scalar value embedded by JsonObjectParts. */
public class JsonString extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public String eval(String value) throws Exception {
        return value == null ? null : MAPPER.writeValueAsString(value);
    }
}
