package org.apache.rocketmq.schema.registry.client;

import org.apache.rocketmq.schema.registry.client.rest.RestService;

import java.util.Map;

public class SchemaRegistryClientFactory {

    public static SchemaRegistryClient newClient(String baseUrl, Map<String, String> map) {
        RestService restService = null == map ? new RestService(baseUrl) : new RestService(baseUrl, map);
        return new NormalSchemaRegistryClient(restService);
    }
}
