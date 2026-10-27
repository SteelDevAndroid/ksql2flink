FROM confluentinc/cp-ksqldb-server:7.9.2 AS reference-libraries
FROM flink:1.20.3-scala_2.12-java17 AS runtime

# The SQL distribution does not bundle the Kafka SQL connector.
USER root
RUN mkdir -p /opt/flink/state && chown flink:flink /opt/flink/state \
    && curl --fail --location --retry 3 \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
      -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar
USER flink
# Exact HyperLogLog implementation used by the pinned reference engine.
COPY --from=reference-libraries /usr/share/java/ksqldb/ksqldb-stream-lib-7.9.2.jar /opt/flink/lib/

FROM eclipse-temurin:17-jdk AS compiler
COPY --from=runtime /opt/flink/lib /deps
COPY src /src
COPY resources /resources
RUN mkdir /classes && javac -cp '/deps/*' -d /classes /src/lab/*.java \
    && jar --create --file /migration-lab.jar -C /classes . -C /resources .

# Unit tests: docker build --build-context sql=sql --target test docker/flink
FROM compiler AS test
ADD https://repo.maven.apache.org/maven2/org/junit/platform/junit-platform-console-standalone/1.11.4/junit-platform-console-standalone-1.11.4.jar /junit.jar
COPY test /test
COPY --from=sql flink /sql
RUN mkdir /test-classes && javac -cp '/classes:/deps/*:/junit.jar' -d /test-classes /test/lab/*.java \
    && java -cp '/test-classes:/classes:/deps/*:/junit.jar' org.junit.platform.console.ConsoleLauncher \
      execute --scan-class-path /test-classes --fail-if-no-tests

FROM runtime
COPY --from=compiler /migration-lab.jar /opt/flink/lib/migration-lab.jar
