package lab;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** Structural membership, independent of JSON whitespace and object-property ordering. */
public class JsonContains extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public Boolean eval(String array, String value) throws Exception {
        if (array == null) return null;
        JsonNode items = MAPPER.readTree(array);
        if (items.isNull()) return null;
        if (!items.isArray()) throw new IllegalArgumentException("Expected JSON array");
        JsonNode needle = value == null ? MAPPER.nullNode() : MAPPER.readTree(value);
        for (JsonNode item : items) if (item.equals(needle)) return true;
        return false;
    }
}
