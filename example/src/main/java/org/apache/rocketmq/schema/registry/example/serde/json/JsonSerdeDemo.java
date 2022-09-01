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

package org.apache.rocketmq.schema.registry.example.serde.json;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.config.JsonSerdeConfig;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.serde.json.JsonSerde;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.apache.rocketmq.schema.registry.example.serde.Person;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonSerdeDemo {
    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        // register schema first, if have registered before ignore
        String topic = "TopicTest";
        RegisterSchemaRequest request = RegisterSchemaRequest.builder()
                .schemaIdl("{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"int\"}}")
                .schemaType(SchemaType.JSON)
                .compatibility(Compatibility.BACKWARD)
                .owner("test").build();
        try {
            RegisterSchemaResponse response
                    = schemaRegistryClient.registerSchema("default", "tanant1", topic, "Person", request);
            System.out.println("register schema success, schemaId: " + response.getRecordId());

            Thread.sleep(5000);
            System.out.println("current schema: " + schemaRegistryClient.getSchemaBySubject(topic));
        } catch (RestClientException | IOException | InterruptedException e) {
            e.printStackTrace();
        }

        Person person = new Person(1L, "name", 18);
        System.out.printf("person before serialize is %s\n", person);

        try(JsonSerde<Person> jsonSerde = new JsonSerde<>(schemaRegistryClient)) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(JsonSerdeConfig.DESERIALIZE_TARGET_TYPE, Person.class);
            jsonSerde.configure(configs);
            byte[] bytes = jsonSerde.serializer().serialize("TopicTest", person);

            Person person1 = jsonSerde.deserializer().deserialize("TopicTest", bytes);
            System.out.printf("after deserialize new person is %s\n", person1);
            System.out.printf("person == person1 : %b\n", person1.equals(person));
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }
    }
}
