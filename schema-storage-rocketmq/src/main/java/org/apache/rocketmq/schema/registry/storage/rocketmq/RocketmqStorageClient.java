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

package org.apache.rocketmq.schema.registry.storage.rocketmq;

import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;

public interface RocketmqStorageClient {

    /**
     * Standard error message for all default implementations.
     */
    String ERROR_MESSAGE_DEFAULT = "Not supported for rocketmq storage client";

    /**
     * Register rocketmq schema entity.
     *
     * @param schemaInfo schema info
     */
    default SchemaInfo register(SchemaInfo schemaInfo) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Delete rocketmq schema entity.
     *
     * @param qualifiedName schema name
     */
    default void delete(QualifiedName qualifiedName) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Update rocketmq schema entity.
     *
     * @param schemaInfo schema info
     */
    default SchemaInfo update(SchemaInfo schemaInfo) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Get rocketmq schema entity.
     *
     * @param qualifiedName schema name
     */
    default SchemaInfo getSchema(QualifiedName qualifiedName) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Get rocketmq schema entity by subject.
     *
     * @param qualifiedName schema name
     */
    default SchemaRecordInfo getBySubject(QualifiedName qualifiedName) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }
}
