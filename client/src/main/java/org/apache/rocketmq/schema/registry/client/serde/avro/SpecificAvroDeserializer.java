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

import org.apache.avro.specific.SpecificRecord;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;

import java.util.Map;

public class SpecificAvroDeserializer<T extends SpecificRecord> implements Deserializer<T> {

    private final AvroDeserializer<SpecificRecord> inner;

    public SpecificAvroDeserializer() {
        this.inner = new AvroDeserializer<>();
    }

    public SpecificAvroDeserializer(final SchemaRegistryClient client) {
        this.inner = new AvroDeserializer<>(client);
    }

    @Override
    public void configure(final Map<String, Object> configs) {
        this.inner.configure(configs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String subject, byte[] bytes) {
        return (T) this.inner.deserialize(subject, bytes);
    }

    @Override
    public void close() {
        this.inner.close();
    }
}
