package org.apache.rocketmq.schema.registry.client.serializer;

import java.io.Closeable;
import java.util.Map;

public interface Serializer<T> extends Closeable {

    default void configure(Map<String, ?> configs) {}

    byte[] serialize(String target, T origin);

    default void close(){}
}
