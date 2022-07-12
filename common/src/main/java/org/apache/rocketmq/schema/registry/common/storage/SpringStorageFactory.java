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

import org.apache.rocketmq.schema.registry.common.context.StoragePluginContext;
import org.apache.rocketmq.schema.registry.common.model.StorageType;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.StandardEnvironment;

public class SpringStorageFactory implements StorageFactory {

    protected final AnnotationConfigApplicationContext ctx;
    private final StoragePluginContext storagePluginContext;
    private final StorageType storageType;

    public SpringStorageFactory(final StoragePluginContext storagePluginContext) {
        this.storagePluginContext = storagePluginContext;
        this.storageType = storagePluginContext.getConfig().getStorageType();
        this.ctx = new AnnotationConfigApplicationContext();
        this.ctx.setEnvironment(new StandardEnvironment());
        this.ctx.getBeanFactory().registerSingleton("StoragePluginContext", storagePluginContext);
    }

    /**
     * register classes to context.
     *
     * @param clazz classes object.
     */
    protected void registerClazz(final Class<?>... clazz) {
        this.ctx.register(clazz);
    }

    /**
     * refresh the context.
     */
    public void refresh() {
        this.ctx.refresh();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageType getStorageType() {
        return this.storageType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        this.ctx.close();
    }
}
