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

package org.apache.rocketmq.schema.registry.example.serde.avro;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.config.AvroSerdeConfig;
import org.apache.rocketmq.schema.registry.client.serde.avro.SpecificAvroSerde;
import org.apache.rocketmq.schema.registry.example.serde.Charge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SpecificAvroSerdeDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);
        Map<String, Object> serializeConfigs = new HashMap<>();


        try (SpecificAvroSerde<Charge> serde = new SpecificAvroSerde<Charge>(schemaRegistryClient)) {

            //serialize
            Charge charge = new Charge("specific", 100.0);
            serializeConfigs.put(AvroSerdeConfig.DESERIALIZE_TARGET_TYPE, charge.getClass());
            serde.configure(serializeConfigs);
            byte[] bytes = serde.serializer().serialize("TopicTest", charge);

            //deserialize
            Charge charge1 = serde.deserializer().deserialize("TopicTest", bytes);
            System.out.println("the origin object after ser/de is " + charge1);
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }

    }
}
