package org.apache.rocketmq.schema.registry.client.serializer;

import org.apache.rocketmq.schema.registry.client.avro.AvroSchema;
import org.apache.rocketmq.schema.registry.client.util.AvroSchemaUtil;

import java.util.Map;

public class AvroSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs) {
        Serializer.super.configure(configs);
    }

    @Override
    public byte[] serialize(String target, T origin) {
        if (origin == null) {
            return null;
        }
        AvroSchema avroSchema = new AvroSchema(AvroSchemaUtil.getSchema(origin));
        return
    }

    @Override
    public void close() {
    }
}
