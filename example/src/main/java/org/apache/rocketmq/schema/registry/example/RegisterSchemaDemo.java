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

package org.apache.rocketmq.schema.registry.example;

import java.io.IOException;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;

public class RegisterSchemaDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        String topic = "TopicTest";
        RegisterSchemaRequest request = RegisterSchemaRequest.builder()
            .schemaIdl("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}")
            .schemaType(SchemaType.AVRO)
            .compatibility(Compatibility.BACKWARD)
            .owner("test").build();
        try {
            RegisterSchemaResponse response
                = schemaRegistryClient.registerSchema(topic, "Charge", request);
            System.out.println("register schema success, schemaId: " + response.getSchemaId());

            Thread.sleep(5000);
            System.out.println("current schema: " + schemaRegistryClient.getSchemaBySubject(topic));
        } catch (RestClientException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
