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
import org.apache.rocketmq.schema.registry.client.serializer.AvroDeserializer;
import org.apache.rocketmq.schema.registry.client.serializer.AvroSerializer;
import org.apache.rocketmq.schema.registry.client.serializer.Deserializer;
import org.apache.rocketmq.schema.registry.client.serializer.Serializer;

public class AvroSerdeDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080/schema-registry/v1";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        //serialize
        Charge charge = new Charge("fee1", 100.0);
        Serializer<Charge> serializer = new AvroSerializer<>(schemaRegistryClient);
        byte[] bytes = serializer.serialize("TopicTest", charge);

        //deserialize
        Deserializer<Charge> deserializer = new AvroDeserializer<>(schemaRegistryClient);
        Charge charge1 = deserializer.deserialize("TopicTest", bytes);
        System.out.println("the origin object after ser/de is " + charge1);
    }
}
