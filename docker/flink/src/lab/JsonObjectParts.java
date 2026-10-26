package lab;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Construct nested JSON from already serialized values, avoiding double encoding. */
public class JsonObjectParts extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public String eval(String[] names, String[] values) throws Exception {
        if (names.length != values.length) throw new IllegalArgumentException("Mismatched field arrays");
        ObjectNode result = MAPPER.createObjectNode();
        for (int i = 0; i < names.length; i++)
            result.set(names[i], values[i] == null ? MAPPER.nullNode() : MAPPER.readTree(values[i]));
        return MAPPER.writeValueAsString(result);
    }
}
