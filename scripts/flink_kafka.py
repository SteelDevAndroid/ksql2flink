"""Kafka connection placeholders shared by the Flink SQL scripts and the lab tests."""
import re

KAFKA_OPTIONS = [
    "'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}'",
    "'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}'",
    "'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}'",
    "'properties.sasl.jaas.config'='${KAFKA_JAAS}'",
]
KAFKA = ',\n  '.join(KAFKA_OPTIONS)

# Lab broker: PLAINTEXT, so the SASL settings are ignored by the Kafka client.
LAB_KAFKA = {
    'KAFKA_BOOTSTRAP': 'kafka:9092',
    'KAFKA_SECURITY_PROTOCOL': 'PLAINTEXT',
    'KAFKA_SASL_MECHANISM': 'PLAIN',
    'KAFKA_JAAS': '',
}


def resolve(sql, env=LAB_KAFKA):
    """Same substitution as lab.SqlRunner: quotes escaped, undefined variables rejected."""
    def value(match):
        if match[1] not in env:
            raise KeyError('Undefined variable: ' + match[1])
        return env[match[1]].replace("'", "''")
    return re.sub(r'\$\{([A-Z0-9_]+)}', value, sql)
