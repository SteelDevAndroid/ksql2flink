package lab;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Translation of the two ksqlDB TRANSFORM lambdas; event selection remains in SQL. */
public class BadgeHistory extends ScalarFunction {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static String text(JsonNode node, String... path) {
        for (String field : path) node = node == null ? null : node.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }
    private static String sub(String value, int start, int length) {
        if (value == null) return "";
        int from = Math.min(start - 1, value.length());
        return value.substring(from, Math.min(from + length, value.length()));
    }
    private static String pad(String value, int length) {
        if (value == null) return null;
        return value.length() >= length ? value.substring(0, length) : "0".repeat(length - value.length()) + value;
    }
    public String eval(String event, String flavor) throws Exception {
        if (event == null) return null;
        boolean cardme = flavor.equals("CARDME");
        if (!cardme && !flavor.equals("CIP")) throw new IllegalArgumentException("Unknown badge type");
        JsonNode items = JSON.readTree(event).get(cardme ? "receiptData" : "D_HIS");
        if (items == null || items.isNull()) return null;
        ArrayNode result = JSON.createArrayNode();
        for (JsonNode item : items) {
            ObjectNode row = JSON.createObjectNode(), location = JSON.createObjectNode(), equipment = JSON.createObjectNode();
            String station = cardme ? sub(text(item, "serviceProvider", "pays"), 1, Integer.MAX_VALUE)
                + sub(text(item, "serviceProvider", "identifiant"), 4, 2)
                + sub(text(item, "locationOfStation"), 3, 3) : text(item, "gare");
            location.put("gare", station);
            equipment.put("numero", cardme ? text(item, "location", "numeroVoie") : pad(text(item, "numeroEquipement"), 3));
            equipment.put("type", ""); location.set("equipement", equipment); row.set("localisation", location);
            String time = text(item, cardme ? "time" : "DHM");
            row.put("DHM", sub(time, 1, 4) + "-" + sub(time, 5, 2) + "-" + sub(time, 7, 2)
                + "T" + sub(time, 9, 2) + ":" + sub(time, 11, 2) + ":" + sub(time, 13, 2));
            row.put("sens", "0".equals(cardme ? text(item, "location", "sens") : text(item, "sens")) ? "E" : "S");
            row.put("typePassage", cardme ? pad(text(item, "type"), 2) : text(item, "typePassage"));
            String amount = cardme ? text(item, "fee", "montant") : text(item, "montant");
            if (amount == null) row.putNull("tarif"); else row.put("tarif", Double.parseDouble(amount) / 100);
            row.put("classe", cardme ? text(item, "tariffClass") : pad(text(item, "classe"), 3));
            result.add(row);
        }
        return JSON.writeValueAsString(result);
    }
}
