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

package org.apache.rocketmq.schema.registry.storage.jdbc;

import org.apache.rocketmq.schema.registry.common.context.StoragePluginContext;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.storage.SpringStorageFactory;
import org.apache.rocketmq.schema.registry.common.storage.StorageService;
import org.apache.rocketmq.schema.registry.storage.jdbc.configs.ServiceConfig;

import java.io.IOException;

public class JdbcStorageFactory
    extends SpringStorageFactory {
    public JdbcStorageFactory(StoragePluginContext context) {
        super(context);
        super.registerClazz(ServiceConfig.class);
        super.refresh();
    }

    @Override
    public StorageService<SchemaInfo> getStorageService() {
        return this.ctx.getBean(JdbcStorageService.class);
    }

    @Override
    public void stop() {
        try {
            getStorageService().close();
        } catch (IOException e) {
            // Ignore this error
        }
        super.stop();
    }
}
