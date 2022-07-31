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

package org.apache.rocketmq.schema.registry.core.api.v1;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.net.HttpURLConnection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;
import org.apache.rocketmq.schema.registry.core.api.RequestProcessor;
import org.apache.rocketmq.schema.registry.core.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * RocketMQ schema registry V1 API implementation.
 */
@RestController
@RequestMapping(
    path = "/schema-registry/v1",
    produces = MediaType.APPLICATION_JSON_VALUE
    )
@Api(
    value = "SchemaRegistryV1",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE
    )
@Slf4j
public class SchemaController {

    private final RequestProcessor requestProcessor;
    private final SchemaService<SchemaDto> schemaService;

    public static final String DEFAULT_TENANT = "default";

    public static final String DEFAULT_CLUSTER = "default";

    /**
     * Constructor.
     *
     * @param requestProcessor request processor
     * @param schemaService    schema service
     */
    @Autowired
    public SchemaController(
        final RequestProcessor requestProcessor,
        final SchemaService<SchemaDto> schemaService
    ) {
        this.requestProcessor = requestProcessor;
        this.schemaService = schemaService;
    }

    @RequestMapping(
        method = RequestMethod.POST,
        path = "/subject/{subject-name}/schema/{schema-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
        )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        value = "Register a new schema",
        notes = "Return success if there were no errors registering the schema"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The schema was registered"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be registered"
            )
        }
        )
    public RegisterSchemaResponse registerSchema(
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable(value = "schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final RegisterSchemaRequest registerSchemaRequest
    ) {
        return registerSchema(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, schemaName, registerSchemaRequest);
    }

    @RequestMapping(
        method = RequestMethod.POST,
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
        )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        value = "Register a new schema",
        notes = "Return success if there were no errors registering the schema"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The schema was registered"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be registered"
            )
        }
        )
    public RegisterSchemaResponse registerSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable(value = "schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final RegisterSchemaRequest registerSchemaDto
    ) {
        // TODO: support register by sql
        final QualifiedName name = new QualifiedName(cluster, tenant, subject, schemaName);

        return this.requestProcessor.processRequest(
            name,
            "register",
            () -> {
                return this.schemaService.register(name, registerSchemaDto);
            }
        );
    }

    @RequestMapping(
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema",
        method = RequestMethod.DELETE
        )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "Delete schema",
        notes = "Delete the schema under the given subject"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "Schema deleted success"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be found or it's still been used"
            )
        }
        )
    public SchemaDto deleteSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, null);
        return this.requestProcessor.processRequest(
            name,
            "deleteSchema",
            () -> this.schemaService.delete(name)
        );
    }

    @RequestMapping(
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions/{version}",
        method = RequestMethod.DELETE
        )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "Delete schema",
        notes = "Delete the schema under the given subject and version"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "Schema deleted success"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be found or it's still been used"
            )
        }
        )
    public SchemaDto deleteSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject,
        @ApiParam(value = "The version of the schema", required = true)
        @PathVariable(value = "version") final String version
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, null, Long.valueOf(version));
        return this.requestProcessor.processRequest(
            name,
            "deleteSchema",
            () -> this.schemaService.delete(name)
        );
    }

    @RequestMapping(
        path = "/subject/{subject-name}/schema/{schema-name}",
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE
        )
    @ApiOperation(
        value = "Update schema and generate new schema version",
        notes = "Update the given schema"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "Update schema success"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be found"
            )
        }
        )
    public UpdateSchemaResponse updateSchema(
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable(value = "schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final UpdateSchemaRequest updateSchemaRequest
    ) {
        return updateSchema(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, schemaName, updateSchemaRequest);
    }

    @RequestMapping(
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}",
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE
        )
    @ApiOperation(
        value = "Update schema and generate new schema version",
        notes = "Update the given schema"
        )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "Update schema success"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested schema cannot be found"
            )
        }
        )
    public UpdateSchemaResponse updateSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(value = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable(value = "schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final UpdateSchemaRequest updateSchemaRequest
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, schemaName);
        return this.requestProcessor.processRequest(
            name,
            "updateSchema",
            () -> this.schemaService.update(name, updateSchemaRequest)
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/subject/{subject-name}/schema"
        )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information with the latest version under the subject")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The schema is returned"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested tenant or schema cannot be found"
            )
        }
        )
    public SchemaRecordDto getSchemaBySubject(
        @ApiParam(value = "The name of the subject", required = true)
        @PathVariable(value = "subject-name") String subject
    ) {
        return getSchemaBySubject(DEFAULT_CLUSTER, DEFAULT_CLUSTER, subject);
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema"
        )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information with the latest version under the subject")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The schema is returned"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested tenant or schema cannot be found"
            )
        }
        )
    public SchemaRecordDto getSchemaBySubject(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The name of the subject", required = true)
        @PathVariable(value = "subject-name") String subject
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, null);
        log.info("Request for get schema for subject: {}", name.subjectFullName());
        return this.requestProcessor.processRequest(
            name,
            "getSchemaByTenantSubject",
            () -> schemaService.getBySubject(name)
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions/{version}"
        )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information with the given version under the subject")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The schema is returned"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested tenant or schema cannot be found"
            )
        }
        )
    public SchemaRecordDto getSchemaBySubject(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The name of the subject", required = true)
        @PathVariable(value = "subject-name") String subject,
        @ApiParam(value = "The version of the schema", required = true)
        @PathVariable(value = "version") String version
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, null, Long.parseLong(version));

        return this.requestProcessor.processRequest(
            "getSchemaByTenantSubject",
            () -> schemaService.getBySubject(name)
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions"
        )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information with a list of versions under the subject")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The schema is returned"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested tenant or schema cannot be found"
            )
        }
        )
    public List<SchemaRecordDto> getSchemaListBySubject(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The name of the subject", required = true)
        @PathVariable(value = "subject-name") String subject
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, null);

        return this.requestProcessor.processRequest(
            "getSchemaListByTenantSubject",
            () -> schemaService.listBySubject(name)
        );
    }
}
