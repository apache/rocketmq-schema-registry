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

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.serde.Charge;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpecificAvroSerdeTest {

    @Mock
    private SchemaRegistryClient registryClient;

    @Mock
    private GetSchemaResponse getSchemaResponse;

    @Test
    public void testSpecificSerde() throws RestClientException, IOException {
        String idl = "{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.client.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}";

        getSchemaResponse = mock(GetSchemaResponse.class);
        when(getSchemaResponse.getRecordId()).thenReturn(11111L);
        when(getSchemaResponse.getSchemaFullName()).thenReturn("org.apache.rocketmq.schema.registry.example.serde.Charge");
        when(getSchemaResponse.getIdl()).thenReturn(idl);

        registryClient = mock(SchemaRegistryClient.class);
        when(registryClient.getSchemaBySubject("TopicTest")).thenReturn(getSchemaResponse);

        try (SpecificAvroSerde serde = new SpecificAvroSerde(registryClient)) {
            //serialize
            Charge charge = new Charge("specific", 100.0);
            byte[] bytes = serde.serializer().serialize("TopicTest", charge);

            //deserialize
            Charge charge1 = (Charge) serde.deserializer().deserialize("TopicTest", bytes);
            assertThat(charge1).isEqualTo(charge);
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }
    }
}
