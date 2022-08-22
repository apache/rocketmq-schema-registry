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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.io.EncoderFactory;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.client.serde.Serializer;
import org.apache.rocketmq.schema.registry.common.constant.SchemaConstants;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final SchemaRegistryClient registryClient;
    private final ObjectMapper objectMapper;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public JsonSerializer(SchemaRegistryClient registryClient) {
        this.objectMapper = JacksonMapper.INSTANCE;
        this.registryClient = registryClient;
    }

    @Override
    public void configure(Map<String, Object> configs) {

    }

    @Override
    public byte[] serialize(String subject, T originMessage) {
        if (null == originMessage) {
            return null;
        }

        if (null == registryClient) {
            throw new SerializationException("please initialize the schema registry client first");
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            GetSchemaResponse response = registryClient.getSchemaBySubject(subject);
            long schemaRecordId = response.getRecordId();
            ByteBuffer buffer = ByteBuffer.allocate(SchemaConstants.SCHEMA_RECORD_ID_LENGTH);
            out.write(buffer.putLong(schemaRecordId).array());
            out.write(objectMapper.writeValueAsBytes(originMessage));

            byte[] bytes = out.toByteArray();
            return bytes;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("JSON serialize failed", e);
        } catch (RestClientException e) {
            throw new SerializationException("get schema by subject failed", e);
        }

    }

    @Override
    public void close() {

    }
}
