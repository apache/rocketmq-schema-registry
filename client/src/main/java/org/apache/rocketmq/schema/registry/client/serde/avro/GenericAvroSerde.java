/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.schema.registry.client.serde.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;
import org.apache.rocketmq.schema.registry.client.serde.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class GenericAvroSerde<T extends GenericRecord> implements Closeable {
    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public GenericAvroSerde() {
        this.serializer = new GenericAvroSerializer<T>();
        this.deserializer = new GenericAvroDeserializer<T>();
    }

    public GenericAvroSerde(final SchemaRegistryClient client) {
        if (null == client) {
            throw  new IllegalArgumentException("please initialize the schema registry client first");
        }
        this.serializer = new AvroSerializer<>(client);
        this.deserializer = new AvroDeserializer<>(client);
    }

    public void configure(final Map<String, Object> configs) {
        this.serializer.configure(configs);
        this.deserializer.configure(configs);
    }

    public Serializer<T> serializer() {
        return this.serializer;
    }

    public Deserializer<T> deserializer() {
        return this.deserializer;
    }

    @Override
    public void close() throws IOException {
        this.serializer.close();
        this.deserializer.close();
    }
}
