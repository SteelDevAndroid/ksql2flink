package lab;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

/** Match the first case-insensitive JSON field, as ksqlDB does. Values stay unchanged. */
public class NormalizeJsonKeys extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String eval(String json) throws Exception {
        if (json == null) return null;
        return MAPPER.writeValueAsString(normalize(MAPPER.readTree(json)));
    }

    private JsonNode normalize(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = MAPPER.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String name = field.getKey().toUpperCase(Locale.ROOT);
                if (!result.has(name)) result.set(name, normalize(field.getValue()));
            }
            return result;
        }
        if (node.isArray()) {
            ArrayNode result = MAPPER.createArrayNode();
            for (JsonNode item : node) result.add(normalize(item));
            return result;
        }
        return node;
    }
}
