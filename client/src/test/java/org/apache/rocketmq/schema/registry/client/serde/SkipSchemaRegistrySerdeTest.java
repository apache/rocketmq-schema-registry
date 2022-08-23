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
package org.apache.rocketmq.schema.registry.client.serde;

import org.apache.rocketmq.schema.registry.client.config.JsonSerializerConfig;
import org.apache.rocketmq.schema.registry.client.serde.json.JsonSerde;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SkipSchemaRegistrySerdeTest {

    @Test
    public void testJsonSerde() {
        Person person = new Person(1L, "name", 18);
        System.out.printf("person before serialize is %s\n", person);

        try(JsonSerde<Person> jsonSerde = new JsonSerde<>()) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(JsonSerializerConfig.SKIP_SCHEMA_REGISTRY, true);
            configs.put(JsonSerializerConfig.DESERIALIZE_TARGET_TYPE, Person.class);
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
