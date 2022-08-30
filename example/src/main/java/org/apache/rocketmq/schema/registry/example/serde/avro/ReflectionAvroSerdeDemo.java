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

import org.apache.rocketmq.schema.registry.client.config.AvroSerializerConfig;
import org.apache.rocketmq.schema.registry.client.serde.avro.ReflectionAvroSerde;
import org.apache.rocketmq.schema.registry.example.serde.Charge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReflectionAvroSerdeDemo {
    public static void main(String[] args) {
        Charge charge = new Charge("specific", 100.0);
        Map<String, Object> configs = new HashMap<>();
        configs.put(AvroSerializerConfig.DESERIALIZE_TARGET_TYPE, charge.getClass());
        try (ReflectionAvroSerde serde = new ReflectionAvroSerde()) {
            //serialize
            serde.configure(configs);

            byte[] bytes = serde.serializer().serialize("TopicTest", charge);
            //deserialize
            Charge charge1 = (Charge) serde.deserializer().deserialize("TopicTest", bytes);
            System.out.println("charge before ser/de is " + charge);
            System.out.println("charge after ser/de is " + charge1);
            System.out.printf("charge == charge1 : %b", charge.equals(charge1));
        } catch (IOException e) {
            System.out.println("serde shutdown failed");
        }
    }
}
