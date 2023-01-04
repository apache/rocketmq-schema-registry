package org.apache.rocketmq.schema.registry.client;

import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.RestService;
import org.apache.rocketmq.schema.registry.common.dto.*;

import java.io.IOException;
import java.util.List;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

    private final RestService restService;

    public CachedSchemaRegistryClient(RestService restService) {
        this.restService = restService;
    }

    @Override
    public RegisterSchemaResponse registerSchema(String subject, String schemaName,
        RegisterSchemaRequest request) throws RestClientException, IOException {
        return restService.registerSchema(subject, schemaName, request);
    }

    @Override
    public RegisterSchemaResponse registerSchema(String clusterName, String tenant, String subjectName,
        String schemaName, RegisterSchemaRequest request) throws IOException, RestClientException {
        return restService.registerSchema(clusterName, tenant, subjectName, schemaName, request);
    }

    @Override
    public DeleteSchemeResponse deleteSchema(String cluster, String tenant,
        String subject) throws IOException, RestClientException {
        return restService.deleteSchema(cluster, tenant, subject);
    }

    @Override
    public DeleteSchemeResponse deleteSchema(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        return restService.deleteSchema(cluster, tenant, subject, version);
    }

    @Override
    public UpdateSchemaResponse updateSchema(String subject, String schemaName,
        UpdateSchemaRequest request) throws RestClientException, IOException {
        return restService.updateSchema(subject, schemaName, request);
    }

    @Override
    public UpdateSchemaResponse updateSchema(String cluster, String tenant, String subjectName,
        String schemaName, UpdateSchemaRequest request) throws IOException, RestClientException {
        return restService.updateSchema(cluster, tenant, subjectName, schemaName, request);
    }

    @Override
    public GetSchemaResponse getSchemaBySubject(String subject) throws RestClientException, IOException {
        return restService.getSchemaBySubject(subject);
    }

    @Override
    public GetSchemaResponse getSchemaBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        return restService.getSchemaBySubject(cluster, tenant, subject);
    }

    @Override
    public GetSchemaResponse getSchemaBySubjectAndVersion(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        return restService.getSchemaBySubject(cluster, tenant, subject, version);
    }

    public GetSchemaResponse getSchemaBySubjectAndVersion(String subject, long version)
        throws IOException, RestClientException {
        return restService.getSchemaBySubject(subject, version);
    }

    @Override
    public GetSchemaResponse getTargetSchema(String cluster, String tenant, String subject, String schema)
        throws RestClientException, IOException {
        return restService.getTargetSchema(cluster, tenant, subject, schema);
    }

    @Override
    public GetSchemaResponse getTargetSchema(String subject, String schema)
        throws RestClientException, IOException {
        return restService.getTargetSchema(subject, schema);
    }

    @Override
    public List<SchemaRecordDto> getSchemaListBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        return restService.getSchemaListBySubject(cluster, tenant, subject);
    }

    @Override
    public List<String> getSubjectsByTenant(String cluster, String tenant)
        throws RestClientException, IOException {
        return restService.getSubjectsByTenant(cluster, tenant);
    }

    @Override
    public List<String> getAllTenants(String cluster) throws RestClientException, IOException {
        return restService.getAllTenants(cluster);
    }

    public GetSchemaResponse getSchemaByRecordId(String cluster, String tenant, String subject,
        long recordId) throws RestClientException, IOException {
        return restService.getSchemaByRecordId(cluster, tenant, subject, recordId);
    }

    @Override
    public GetSchemaResponse getSchemaByRecordId(String subject, long recordId)
        throws RestClientException, IOException {
        return restService.getSchemaByRecordId(subject, recordId);
    }
}
