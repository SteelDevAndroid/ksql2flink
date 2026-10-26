package lab;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Schema serialization for the valid STRING/INTEGER/DOUBLE/STRUCT/ARRAY input domain. */
public class ProjectKsqlJson extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, JsonNode> SCHEMAS = new ConcurrentHashMap<>();

    public String eval(String json, String direction) throws Exception {
        if (json == null) return null;
        String name = direction.equals("entree") || direction.equals("sortie")
            ? "conformite-" + direction : direction;
        if (!name.matches("[a-z0-9-]+")) throw new IllegalArgumentException("Invalid schema name");
        JsonNode schema = SCHEMAS.get(name);
        if (schema == null) {
            try (InputStream input = ProjectKsqlJson.class.getResourceAsStream("/schemas/" + name + ".json")) {
                if (input == null) throw new IllegalArgumentException("Missing schema: " + name);
                schema = MAPPER.readTree(input);
                SCHEMAS.put(name, schema);
            }
        }
        return MAPPER.writeValueAsString(project(MAPPER.readTree(json), schema));
    }

    private JsonNode project(JsonNode value, JsonNode schema) {
        if (value == null || value.isNull()) return MAPPER.nullNode();
        if (schema.isObject()) {
            if (!value.isObject()) throw new IllegalArgumentException("Expected STRUCT");
            Map<String, JsonNode> fields = new HashMap<>();
            value.fields().forEachRemaining(f -> fields.putIfAbsent(f.getKey().toUpperCase(Locale.ROOT), f.getValue()));
            ObjectNode result = MAPPER.createObjectNode();
            // In this journal uppercase names are unquoted; other names are quoted.
            // Quoted identifiers must match the original JSON property exactly.
            schema.fields().forEachRemaining(f -> result.set(f.getKey(), project(
                f.getKey().equals(f.getKey().toUpperCase(Locale.ROOT))
                    ? fields.get(f.getKey()) : value.get(f.getKey()), f.getValue())));
            return result;
        }
        if (schema.isArray()) {
            if (!value.isArray()) throw new IllegalArgumentException("Expected ARRAY");
            ArrayNode result = MAPPER.createArrayNode();
            for (JsonNode item : value) result.add(project(item, schema.get(0)));
            return result;
        }
        switch (schema.asText()) {
            case "STRING":
                if (value.isTextual()) return value;
                break;
            case "INTEGER":
                if (value.isIntegralNumber() && value.canConvertToInt()) return MAPPER.getNodeFactory().numberNode(value.intValue());
                break;
            case "DOUBLE":
                if (value.isNumber()) return MAPPER.getNodeFactory().numberNode(value.doubleValue());
                break;
            default: throw new IllegalArgumentException("Unknown schema type");
        }
        throw new IllegalArgumentException("Input does not match declared type: " + schema.asText());
    }
}
