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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;
import org.apache.rocketmq.schema.registry.common.constant.SchemaConstants;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    Logger log = LoggerFactory.getLogger(JsonDeserializer.class);
    private final SchemaRegistryClient registryClient;
    private final ObjectMapper objectMapper;
    private final Class<T> type;

    public JsonDeserializer(SchemaRegistryClient registryClient, Class<T> type) {
        this.registryClient = registryClient;
        objectMapper = JacksonMapper.INSTANCE;
        this.type = type;
    }

    @Override
    public void configure(Map<String, Object> configs) {

    }

    @Override
    public T deserialize(String subject, byte[] payload) {
        if (null == registryClient) {
            throw new SerializationException("please initialize the schema registry client first");
        }

        if (null == payload || payload.length == 0) {
            return null;
        }

        try {
            GetSchemaResponse response = registryClient.getSchemaBySubject(subject);
            ByteBuffer buffer = ByteBuffer.wrap(payload);

            long schemaRecordId = buffer.getLong();

            int length = buffer.limit() - SchemaConstants.SCHEMA_RECORD_ID_LENGTH;
            int start = buffer.position() + buffer.arrayOffset();

            JsonNode jsonNode = null;
            jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);

            return objectMapper.convertValue(jsonNode, type);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
