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

public class GenericAvroSerde implements Closeable {
    private final Serializer<GenericRecord> serializer;
    private final Deserializer<GenericRecord> deserializer;

    public GenericAvroSerde() {
        this.serializer = new GenericAvroSerializer();
        this.deserializer = new GenericAvroDeserializer();
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

    public Serializer<GenericRecord> serializer() {
        return this.serializer;
    }

    public Deserializer<GenericRecord> deserializer() {
        return this.deserializer;
    }

    @Override
    public void close() throws IOException {
        this.serializer.close();
        this.deserializer.close();
    }
}
