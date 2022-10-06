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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.config.AvroSerdeConfig;
import org.apache.rocketmq.schema.registry.client.serde.avro.GenericAvroSerde;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenericAvroSerdeDemo {

    public static void main(String[] args) {

        String baseUrl = "http://localhost:8080";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\",\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}");
        GenericRecord record = new GenericRecordBuilder(schema)
            .set("item", "generic")
            .set("amount", 100.0)
            .build();

        try (GenericAvroSerde serde = new GenericAvroSerde(schemaRegistryClient)) {
            //configure
            Map<String, Object> configs = new HashMap<>();
            configs.put(AvroSerdeConfig.USE_GENERIC_DATUM_READER, true);
            serde.configure(configs);

            //serialize
            byte[] bytes = serde.serializer().serialize("TopicTest", record);

            //deserialize
            GenericRecord record1 = serde.deserializer().deserialize("TopicTest", bytes);
            System.out.println("the origin object after ser/de is " + record1);
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }
    }
}