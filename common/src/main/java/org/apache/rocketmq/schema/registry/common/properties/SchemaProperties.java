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

package org.apache.rocketmq.schema.registry.common.properties;

import lombok.Data;
import lombok.NonNull;

@Data
public class SchemaProperties {

    @NonNull
    private CacheProperties cache = new CacheProperties();

    @NonNull
    private DependencyProperties dependency = new DependencyProperties();

    @NonNull
    private AclProperties acl = new AclProperties();

    @NonNull
    private StorageProperties storage = new StorageProperties();

    @NonNull
    private ServiceProperties service = new ServiceProperties();
}
