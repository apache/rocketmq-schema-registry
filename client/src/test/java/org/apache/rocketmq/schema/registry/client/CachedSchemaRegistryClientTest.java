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

package org.apache.rocketmq.schema.registry.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.RestService;
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CachedSchemaRegistryClientTest {

    private final static String baseUrl = "http://localhost:8080";
    private final static String topic = "TopicTest";

    static RestService restService;
    static NormalSchemaRegistryClient normalSchemaRegistryClient;
    static CachedSchemaRegistryClient cachedSchemaRegistryClient;

    static CachedSchemaRegistryClient cachedSchemaRegistryClientCapacity2;

    @BeforeAll
    static void setUp() {
        restService = new RestService(baseUrl);
        normalSchemaRegistryClient = new NormalSchemaRegistryClient(restService);
        cachedSchemaRegistryClient = new CachedSchemaRegistryClient(restService);
        cachedSchemaRegistryClientCapacity2 = new CachedSchemaRegistryClient(restService, 2, TimeUnit.MINUTES, 2);
        try {
            DeleteSchemeResponse response1
                = cachedSchemaRegistryClient.deleteSchema("default", "default", topic);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
        registerSchema();
    }

    static void registerSchema() {
        RegisterSchemaRequest request = RegisterSchemaRequest.builder()
            .schemaIdl("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}")
            .schemaType(SchemaType.AVRO)
            .compatibility(Compatibility.BACKWARD)
            .owner("test").build();
        try {
            RegisterSchemaResponse response
                = cachedSchemaRegistryClient.registerSchema(topic, "Charge", request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getSchemaBySubject() {
        try {
            GetSchemaResponse normalResponse = normalSchemaRegistryClient.getSchemaBySubject(topic);
            GetSchemaResponse cachedResponse = cachedSchemaRegistryClient.getSchemaBySubject(topic);
            GetSchemaResponse cachedResponse2 = cachedSchemaRegistryClient.getSchemaBySubject(topic);
            GetSchemaResponse cachedResponse3 = cachedSchemaRegistryClient.getSchemaBySubject("default", "default", topic);

            assertEquals(normalResponse, cachedResponse2);
            assertEquals(cachedResponse3, cachedResponse);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getSchemaBySubjectAndVersion() {
        try {
            GetSchemaResponse normalResponse = normalSchemaRegistryClient.getSchemaBySubjectAndVersion("default", "default", topic, 1);
            GetSchemaResponse cachedResponse = cachedSchemaRegistryClient.getSchemaBySubjectAndVersion("default", "default", topic, 1);
            GetSchemaResponse cachedResponse2 = cachedSchemaRegistryClient.getSchemaBySubjectAndVersion("default", "default", topic, 1);
            GetSchemaResponse cachedResponse3 = cachedSchemaRegistryClient.getSchemaBySubjectAndVersion(topic, 1);

            assertEquals(normalResponse, cachedResponse2);
            assertEquals(cachedResponse3, cachedResponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void getSchemaBySubjectAndId() {
        try {
            GetSchemaResponse normalResponse = normalSchemaRegistryClient.getSchemaByRecordId("default", "default", topic, Long.parseLong("135023078756319233"));
            GetSchemaResponse cachedResponse = cachedSchemaRegistryClient.getSchemaByRecordId("default", "default", topic, Long.parseLong("135023078756319233"));
            GetSchemaResponse cachedResponse2 = cachedSchemaRegistryClient.getSchemaByRecordId("default", "default", topic, Long.parseLong("135023078756319233"));
            GetSchemaResponse cachedResponse3 = cachedSchemaRegistryClient.getSchemaByRecordId(topic, Long.parseLong("135023078756319233"));

            assertEquals(normalResponse, cachedResponse2);
            assertEquals(cachedResponse3, cachedResponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void getSchemaBySubjectWithCapacity2() {
        RegisterSchemaRequest request = RegisterSchemaRequest.builder()
            .schemaIdl("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\","
                + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}")
            .schemaType(SchemaType.AVRO)
            .compatibility(Compatibility.BACKWARD)
            .owner("test").build();
        try {
            DeleteSchemeResponse responseD1
                = cachedSchemaRegistryClient.deleteSchema("default", "default", "TopicTest2");
            DeleteSchemeResponse responseD2
                = cachedSchemaRegistryClient.deleteSchema("default", "default", "TopicTest3");
            RegisterSchemaResponse response
                = cachedSchemaRegistryClient.registerSchema("TopicTest2", "Charge2", request);
            RegisterSchemaResponse response2
                = cachedSchemaRegistryClient.registerSchema("TopicTest3", "Charge3", request);
            GetSchemaResponse normalResponse = normalSchemaRegistryClient.getSchemaBySubject(topic);
            GetSchemaResponse normalResponse2 = normalSchemaRegistryClient.getSchemaBySubject("TopicTest2");
            GetSchemaResponse normalResponse3 = normalSchemaRegistryClient.getSchemaBySubject("TopicTest3");

            GetSchemaResponse cachedResponse = cachedSchemaRegistryClientCapacity2.getSchemaBySubject(topic);
            GetSchemaResponse cachedResponse2 = cachedSchemaRegistryClientCapacity2.getSchemaBySubject("TopicTest2");
            GetSchemaResponse cachedResponse3 = cachedSchemaRegistryClientCapacity2.getSchemaBySubject("TopicTest3");

            assertEquals(normalResponse, cachedResponse);
            assertEquals(normalResponse2, cachedResponse2);
            assertEquals(normalResponse3, cachedResponse3);
        } catch (Exception e) {
            e.printStackTrace();
        }

        
    }
    
}