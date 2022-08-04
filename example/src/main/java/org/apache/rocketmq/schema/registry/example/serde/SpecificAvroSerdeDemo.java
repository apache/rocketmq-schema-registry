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

package org.apache.rocketmq.schema.registry.example.serde;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.serde.avro.SpecificAvroSerde;

import java.io.IOException;

public class SpecificAvroSerdeDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080/schema-registry/v1";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        try (SpecificAvroSerde serde = new SpecificAvroSerde(schemaRegistryClient)) {
            //serialize
            Charge charge = new Charge("specific", 100.0);
            byte[] bytes = serde.serializer().serialize("TopicTest", charge);

            //deserialize
            Charge charge1 = (Charge) serde.deserializer().deserialize("TopicTest", bytes);
            System.out.println("the origin object after ser/de is " + charge1);
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }

    }
}
