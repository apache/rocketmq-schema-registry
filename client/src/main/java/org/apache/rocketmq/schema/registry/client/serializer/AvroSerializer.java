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

package org.apache.rocketmq.schema.registry.client.serializer;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;

import java.util.Map;

public class AvroSerializer<T> extends AbstractAvroSerializer implements Serializer<T> {

    public AvroSerializer() {}

    public AvroSerializer(SchemaRegistryClient schemaRegistryClient) {
        schemaRegistry = schemaRegistryClient;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Serializer.super.configure(configs);
    }

    @Override
    public byte[] serialize(String subject, T originMessage) {
        if (originMessage == null) {
            return null;
        }
        return serializeImpl(subject, originMessage);
    }

    @Override
    public void close() {
    }
}
