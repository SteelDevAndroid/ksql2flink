package lab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqlRunnerTest {

    @Test
    void splitIgnoresSemicolonsInLiteralsIdentifiersAndComments() {
        var script = """
                SET 'a' = 'x;y';
                -- comment; ignored
                INSERT INTO `t;1` SELECT 'it''s;ok' FROM s;
                """;

        assertEquals(List.of("SET 'a' = 'x;y'", "INSERT INTO `t;1` SELECT 'it''s;ok' FROM s"), SqlRunner.split(script));
    }

    @Test
    void splitKeepsStatementSetBoundaries() {
        var script = "EXECUTE STATEMENT SET\nBEGIN\nINSERT INTO a SELECT 1;\nINSERT INTO b SELECT 2;\nEND;";

        assertEquals(
                List.of("EXECUTE STATEMENT SET\nBEGIN\nINSERT INTO a SELECT 1", "INSERT INTO b SELECT 2", "END"),
                SqlRunner.split(script));
    }

    @Test
    void splitParsesEveryShippedScript() throws IOException {
        var scripts = Path.of(System.getProperty("sql.dir", "/sql"));
        try (var files = Files.list(scripts)) {
            var sqlFiles = files.filter(p -> p.toString().endsWith(".sql")).toList();
            assertTrue(!sqlFiles.isEmpty(), "no SQL script found in " + scripts);
            for (var file : sqlFiles) {
                var statements = SqlRunner.split(Files.readString(file));
                long begins = statements.stream().filter(s -> s.toUpperCase().startsWith("EXECUTE STATEMENT SET")).count();
                long ends = statements.stream().filter("END"::equalsIgnoreCase).count();
                assertEquals(begins, ends, "unbalanced STATEMENT SET in " + file);
            }
        }
    }

    @Test
    void resolveEscapesQuotes() {
        assertEquals("'p''w'", SqlRunner.resolve("'${PWD}'", Map.of("PWD", "p'w")));
    }

    @Test
    void resolveFailsOnMissingVariable() {
        var error = assertThrows(IllegalArgumentException.class, () -> SqlRunner.resolve("${KAFKA_BOOTSTRAP}", Map.of()));
        assertTrue(error.getMessage().contains("KAFKA_BOOTSTRAP"));
    }
}
