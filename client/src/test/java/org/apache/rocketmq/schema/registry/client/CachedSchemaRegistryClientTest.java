package org.apache.rocketmq.schema.registry.client;

import java.io.IOException;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.RestService;
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.assertj.core.api.Assert;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CachedSchemaRegistryClientTest {

    private final static String baseUrl = "http://localhost:8080";
    private final static String topic = "TopicTest";

    static RestService restService;
    static NormalSchemaRegistryClient normalSchemaRegistryClient;
    static CachedSchemaRegistryClient cachedSchemaRegistryClient;

    @BeforeAll
    static void setUp() {
        restService = new RestService(baseUrl);
        normalSchemaRegistryClient = new NormalSchemaRegistryClient(restService);
        cachedSchemaRegistryClient = new CachedSchemaRegistryClient(restService);
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
                = normalSchemaRegistryClient.registerSchema(topic, "Charge", request);
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
            assertEquals(normalResponse, cachedResponse2);
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
            assertEquals(normalResponse, cachedResponse2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}