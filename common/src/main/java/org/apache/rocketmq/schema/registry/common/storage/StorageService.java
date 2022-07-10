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

package org.apache.rocketmq.schema.registry.common.storage;

import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.model.BaseInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;

public interface StorageService<T extends BaseInfo> {

    /**
     * Error message for all default implementations.
     */
    String ERROR_MESSAGE_DEFAULT = "Not supported method for this storage type";

    /**
     * Register a brand new schema.
     *
     * @param context  The storage service needed context
     * @param schemaInfo The schema information
     * @throws UnsupportedOperationException If the storage type doesn't implement this method
     */
    default SchemaInfo register(final StorageServiceContext context, final T schemaInfo) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Delete schema with all version
     *
     * @param context The storage service needed context
     * @param name Qualified name with tenant / name of schema
     * @throws UnsupportedOperationException If the storage type doesn't implement this method
     */
    default void delete(final StorageServiceContext context, final QualifiedName name) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Update schema and generate a new version with the given information.
     *
     * @param context  The request context
     * @param schemaInfo schema information
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default T update(final StorageServiceContext context, final T schemaInfo) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Query a resource with the given tenant and name.
     *
     * @param context The storage service needed context
     * @param name Qualified name with tenant / name of schema
     * @return The resource metadata.
     * @throws UnsupportedOperationException If the storage type doesn't implement this method
     */
    default T get(final StorageServiceContext context, final QualifiedName name) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    default SchemaRecordInfo getBySubject(final StorageServiceContext context, final QualifiedName name) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }
}
