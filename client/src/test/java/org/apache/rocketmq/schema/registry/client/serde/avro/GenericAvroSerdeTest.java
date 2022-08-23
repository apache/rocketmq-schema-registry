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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.config.AvroSerializerConfig;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GenericAvroSerdeTest {
    @Mock
    private SchemaRegistryClient registryClient;

    @Mock
    private GetSchemaResponse getSchemaResponse;

    @Test
    public void testGenericSerde() throws RestClientException, IOException {
        String idl = "{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}";
        Schema schema = new Schema.Parser().parse(idl);

        getSchemaResponse = mock(GetSchemaResponse.class);
        when(getSchemaResponse.getRecordId()).thenReturn(1111L);
        when(getSchemaResponse.getIdl()).thenReturn(idl);

        registryClient = mock(SchemaRegistryClient.class);
        when(registryClient.getSchemaBySubject("TopicTest")).thenReturn(getSchemaResponse);

        GenericRecord record = new GenericRecordBuilder(schema)
                .set("item", "generic")
                .set("amount", 100.0)
                .build();

        try (GenericAvroSerde serde = new GenericAvroSerde(registryClient)) {
            //configure
            Map<String, Object> configs = new HashMap<>();
            configs.put(AvroSerializerConfig.USE_GENERIC_DATUM_READER, true);
            serde.configure(configs);

            //serialize
            byte[] bytes = serde.serializer().serialize("TopicTest", record);

            //deserialize
            GenericRecord record1 = serde.deserializer().deserialize("TopicTest", bytes);
            assertThat(record1).isEqualTo(record);
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }
    }
}
