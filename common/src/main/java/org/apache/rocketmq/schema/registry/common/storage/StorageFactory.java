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

import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.StorageType;

public interface StorageFactory {

    /**
     * Standard error message for all default implementations.
     */
    String ERROR_MESSAGE_DEFAULT = "Not supported by this storage layer";

    /**
     * Returns the storage service implementation of the config type.
     *
     * @return Returns the storage service implementation of the config type.
     */
    default StorageService<SchemaInfo> getStorageService() {
        throw new UnsupportedOperationException(ERROR_MESSAGE_DEFAULT);
    }

    /**
     * Returns the type of the storage.
     *
     * @return Returns the type of the storage.
     */
    StorageType getStorageType();

    /**
     * Stop and clear the factory.
     */
    void stop();

}
