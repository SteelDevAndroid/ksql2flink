package lab;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** Expose array items to SQL UNNEST without changing order, null items or duplicates. */
public class JsonElements extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public String[] eval(String json) throws Exception {
        if (json == null) return null;
        JsonNode array = MAPPER.readTree(json);
        if (array.isNull()) return null;
        if (!array.isArray()) throw new IllegalArgumentException("Expected JSON array");
        String[] result = new String[array.size()];
        for (int i = 0; i < result.length; i++) result[i] = MAPPER.writeValueAsString(array.get(i));
        return result;
    }
}
