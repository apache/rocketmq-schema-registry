package org.apache.rocketmq.schema.registry.client.avro;


import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class AvroSchema {

    private static final Logger log = LoggerFactory.getLogger(AvroSchema.class);

    public static final String TYPE = "AVRO";

    private final Schema schema;

    private final Long version;

    public AvroSchema(String schemaIdl) {
        this(schemaIdl, null);
    }

    public AvroSchema(String schemaIdl, Long version) {
        Schema.Parser parser = getParser();
        this.schema = parser.parse(schemaIdl);
        this.version = version;
    }

    public AvroSchema(Schema schema) {
        this(schema, null);
    }

    public AvroSchema(Schema schema, Long version) {
        this.schema = schema;
        this.version = version;
    }

    public AvroSchema copy() {
        return new AvroSchema(this.schema, this.version);
    }

    protected Schema.Parser getParser() {
        return new Schema.Parser();
    }

    public Schema getSchema() {
        return this.schema;
    }

    public String getSchemaType() {
        return TYPE;
    }

    public String getSchemaName() {
        if (null != schema && schema.getType() == Schema.Type.RECORD) {
            return schema.getFullName();
        } else {
            return null;
        }
    }

    public Long getVersion() {
        return this.version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvroSchema that = (AvroSchema) o;
        return Objects.equals(schema, that.schema) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, version);
    }
}
