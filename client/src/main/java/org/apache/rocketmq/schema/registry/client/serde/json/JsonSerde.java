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

package org.apache.rocketmq.schema.registry.client.serde.json;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Closeable {
    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    public JsonSerde() {
        this.serializer = new JsonSerializer<>();
        this.deserializer = new JsonDeserializer<>();
    }

    public JsonSerde(SchemaRegistryClient registryClient) {
        this.serializer = new JsonSerializer<>(registryClient);
        this.deserializer = new JsonDeserializer<>(registryClient);
    }

    public void configure(final Map<String, Object> configs) {
        this.serializer.configure(configs);
        this.deserializer.configure(configs);
    }

    public JsonSerializer<T> serializer() {
        return this.serializer;
    }

    public JsonDeserializer<T> deserializer() {
        return this.deserializer;
    }

    @Override
    public void close() throws IOException {

    }
}
