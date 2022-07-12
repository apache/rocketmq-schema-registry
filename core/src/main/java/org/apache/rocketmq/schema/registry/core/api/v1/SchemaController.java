/**
 * Copyright 2022, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package org.apache.rocketmq.schema.registry.core.api.v1;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.net.HttpURLConnection;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.exception.SchemaNotFoundException;
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

    /**
     * Constructor.
     *
     * @param requestProcessor request processor
     * @param schemaService schema service
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
        path = "/cluster/{cluster-name}/subject/{subject-name}/schema/{schema-name}",
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
    public SchemaDto registerSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(name = "cluster-name") final String clusterName,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(name = "subject-name") final String subjectName,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final SchemaDto schemaDto
    ) {
        return registerSchema(clusterName, "default", subjectName, schemaName, schemaDto);
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
    public SchemaDto registerSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(name = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final SchemaDto schemaDto
    ) {
        // TODO: support register by sql
        final QualifiedName name = new QualifiedName(cluster, tenant, subject, schemaName);
        schemaDto.setQualifiedName(name);

        return this.requestProcessor.processRequest(
            name,
            "register",
            () -> {
                return this.schemaService.register(name, schemaDto);
            }
        );
    }

    @RequestMapping(
        path = "/tenant/{tenant-name}/schema/{schema-name}",
        method = RequestMethod.DELETE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "Delete schema",
        notes = "Delete the schema under the given tenant"
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
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable("tenant-name") final String tenant,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName
    ) {
        QualifiedName name = new QualifiedName(null, tenant, null, schemaName);
        return this.requestProcessor.processRequest(
            name,
            "deleteSchema",
            () -> this.schemaService.delete(name)
        );
    }

    @RequestMapping(
        path = "/cluster/{cluster-name}/subject/{subject-name}/schema/{schema-name}",
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
    public SchemaDto updateSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable("cluster-name") final String cluster,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable("subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final SchemaDto schemaDto
    ) {
        return this.updateSchema(cluster, "default", subject, schemaName, schemaDto);
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
    public SchemaDto updateSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(name = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName,
        @ApiParam(value = "The schema detail", required = true)
        @RequestBody final SchemaDto schemaDto
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, schemaName);
        return this.requestProcessor.processRequest(
            name,
            "updateSchema",
            () -> this.schemaService.update(name, schemaDto)
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}"
    )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information for the given schema name under the tenant")
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
    public SchemaDto getSchema(
        @ApiParam(value = "The cluster of the subject", required = true)
        @PathVariable(value = "cluster-name") final String cluster,
        @ApiParam(value = "The tenant of the schema", required = true)
        @PathVariable(value = "tenant-name") final String tenant,
        @ApiParam(value = "The subject of the schema", required = true)
        @PathVariable(name = "subject-name") final String subject,
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("schema-name") final String schemaName
    ) {
        QualifiedName name = new QualifiedName(cluster, tenant, subject, schemaName);
        log.info("Request for get schema for schema: {}", name.fullName());
        return this.requestProcessor.processRequest(
            name,
            "getSchema",
            () -> schemaService.get(name)
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/subject/{subject-name}"
    )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information for the given schema name under the subject")
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
        @ApiParam(value = "The name of the schema", required = true)
        @PathVariable("subject-name") String subject
    ) {
        return getSchemaBySubject("default", subject);
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/cluster/{cluster-name}/subject/{subject-name}"
    )
    @ApiOperation(
        value = "Schema information",
        notes = "Schema information for the given schema name under the subject")
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
        @PathVariable("cluster-name") String cluster,
        @ApiParam(value = "The name of the subject", required = true)
        @PathVariable("subject-name") String subject
    ) {
        QualifiedName name = new QualifiedName(cluster, null, subject, null);

        return this.requestProcessor.processRequest(
            "getSchemaBySubject",
            () -> schemaService.getBySubject(name)
        );
    }
}
