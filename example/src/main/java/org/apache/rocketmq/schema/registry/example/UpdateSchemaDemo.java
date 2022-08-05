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
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

public class UpdateSchemaDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080/schema-registry/v1";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        String topic = "TopicTest";
        UpdateSchemaRequest request = UpdateSchemaRequest.builder()
            .schemaIdl("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}")
            .build();
        try {
            UpdateSchemaResponse response
                = schemaRegistryClient.updateSchema(topic, "Charge", request);
            System.out.println("update schema success, schemaId: " + response.getRecordId());

            Thread.sleep(5000);
            System.out.println("current schema: " + schemaRegistryClient.getSchemaBySubject(topic));
        } catch (RestClientException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
