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

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.model.PluginLoadState;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.context.StoragePluginContext;
import org.springframework.context.ApplicationContext;

@Slf4j
public class StorageManager {

    private final GlobalConfig config;

    private StoragePlugin plugin;
    private StorageFactory factory;

    private final AtomicReference<PluginLoadState> state = new AtomicReference<>(PluginLoadState.INIT);
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public StorageManager(final GlobalConfig config) {
        this.config = config;
    }

    /**
     * Returns the storage service for the configuration
     *
     * @return Returns the storage service
     */
    public StorageService<SchemaInfo> getStorageService() {
        return this.factory.getStorageService();
    }

    /**
     * Loads the storage plugin
     *
     * @throws Exception
     */
    public void loadPlugin() {
        if (!state.compareAndSet(PluginLoadState.INIT, PluginLoadState.LOADING)) {
            return;
        }

        final ServiceLoader<StoragePlugin> serviceLoader =
            ServiceLoader.load(StoragePlugin.class, this.getClass().getClassLoader());
        for (StoragePlugin storagePlugin : serviceLoader) {
            if (config.getStorageType().equals(storagePlugin.getType())) {
                log.info("Loading plugin {}", storagePlugin.getClass().getName());
                this.plugin = storagePlugin;
                log.info("Finished loading plugin {}", storagePlugin.getClass().getName());
            }
        }

        state.set(PluginLoadState.LOADED);
    }

    /**
     * Load storage and creates a connection.
     *
     * @param context schema storage service context
     */
    public void start(ApplicationContext context) {
        if (!state.compareAndSet(PluginLoadState.LOADED, PluginLoadState.STARTING)) {
            return;
        }

        // TODO 1. string constants
        // TODO 2. validate
        // TODO 3. encrypt / decrypt
        StoragePluginContext pluginContext = new StoragePluginContext(config);
        if (plugin != null) {
            factory = plugin.load(pluginContext);
            log.info("factory is loading" + factory);
        } else {
            log.warn("No plugin for storage with type {}", pluginContext.getConfig().getStorageType());
        }

        state.set(PluginLoadState.STARTED);
    }

    /**
     * Stop the storage.
     */
    @PreDestroy
    public void stop() {
        if (stopped.getAndSet(true)) {
            return;
        }

        try {
            factory.stop();
        } catch (Exception e) {
            log.error("Error shutting down storage: {}", factory.getStorageType(), e);
        }
    }

    public boolean isPluginLoaded() {
        return state.get().equals(PluginLoadState.LOADED);
    }

    public boolean isConnected() {
        return connected.get();
    }
}
