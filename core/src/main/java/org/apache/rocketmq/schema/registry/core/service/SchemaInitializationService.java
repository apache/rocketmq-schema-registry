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

package org.apache.rocketmq.schema.registry.core.service;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.storage.StorageManager;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

@Slf4j
public class SchemaInitializationService implements HealthIndicator {

    protected static final String PLUGIN_LOAD_KEY = "pluginLoaded";
    protected static final String STORAGE_CONNECT_KEY = "storageConnected";
    protected static final String THRIFT_KEY = "thrift";

    @Nonnull
    private final StorageManager storageManager;

    public SchemaInitializationService(@Nonnull StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final boolean isPluginLoaded = storageManager.isPluginLoaded();
        final boolean isConnected = storageManager.isConnected();
        final Health.Builder builder = (isPluginLoaded && isConnected) ? Health.up() : Health.outOfService();

        return builder
            .withDetail(PLUGIN_LOAD_KEY, isPluginLoaded)
            .withDetail(STORAGE_CONNECT_KEY, isConnected)
            .build();
    }

    /**
     * Shutdown schema service.
     *
     * @param event Shutting down event
     */
    @EventListener
    public void stop(ContextClosedEvent event) {
        log.info("Stopping schema service by {}.", event);
        try {
            this.storageManager.stop();
        } catch (Exception e) {
            log.error("Stop failed since {}", e.getMessage(), e);
        }
        log.info("Schema service finished.");
    }

    /**
     * Init schema service.
     *
     * @param event Starting Event
     */
    @EventListener
    public void start(ContextRefreshedEvent event) {
        log.info("Starting schema service by {}.", event);
        try {
            this.storageManager.loadPlugin();
            this.storageManager.start(event.getApplicationContext());
        } catch (Exception e) {
            log.error("Unable to initialize services due to {}", e.getMessage(), e);
        }
        log.info("Finished starting internal services.");
    }
}
