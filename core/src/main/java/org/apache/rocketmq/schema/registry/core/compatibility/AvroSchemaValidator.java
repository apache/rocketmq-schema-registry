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

package org.apache.rocketmq.schema.registry.core.compatibility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.rocketmq.schema.registry.common.exception.SchemaCompatibilityException;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;

import static org.apache.rocketmq.schema.registry.common.model.Compatibility.BACKWARD;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.BACKWARD_TRANSITIVE;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.FORWARD;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.FORWARD_TRANSITIVE;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.FULL;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.FULL_TRANSITIVE;

public class AvroSchemaValidator implements org.apache.rocketmq.schema.registry.core.compatibility.SchemaValidator {

    private static final Map<Compatibility, SchemaValidator> SCHEMA_VALIDATOR_CACHE = new HashMap<>(6);

    public AvroSchemaValidator() {
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(BACKWARD,
            new SchemaValidatorBuilder().canReadStrategy().validateLatest());
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(BACKWARD_TRANSITIVE,
            new SchemaValidatorBuilder().canReadStrategy().validateAll());
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(FORWARD,
            new SchemaValidatorBuilder().canBeReadStrategy().validateLatest());
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(FORWARD_TRANSITIVE,
            new SchemaValidatorBuilder().canBeReadStrategy().validateAll());
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(FULL,
            new SchemaValidatorBuilder().mutualReadStrategy().validateLatest());
        SCHEMA_VALIDATOR_CACHE.putIfAbsent(FULL_TRANSITIVE,
            new SchemaValidatorBuilder().mutualReadStrategy().validateAll());
    }

    @Override
    public void validate(SchemaInfo update, SchemaInfo current) {
        Schema toValidate = new Schema.Parser().parse(update.getLastRecordIdl());
        List<Schema> existingList = new ArrayList<>();
        for (String schemaIdl : current.getAllRecordIdlInOrder()) {
            Schema existing = new Schema.Parser().parse(schemaIdl);
            if (existing.equals(toValidate)) {
                throw new SchemaCompatibilityException("The same schemaIdl exists in the previous versions");
            }
            existingList.add(existing);
        }

        org.apache.avro.SchemaValidator validator =
            SCHEMA_VALIDATOR_CACHE.get(current.getMeta().getCompatibility());
        try {
            validator.validate(toValidate, existingList);
        } catch (SchemaValidationException e) {
            throw new SchemaCompatibilityException("Schema compatibility validation failed", e);
        }
    }

}
