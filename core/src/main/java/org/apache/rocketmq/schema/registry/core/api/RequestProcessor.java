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

package org.apache.rocketmq.schema.registry.core.api;

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RequestProcessor {

    @Autowired
    public RequestProcessor() {

    }

    /**
     * Request processor to handle request.
     *
     * @param qualifiedName qualifiedName
     * @param requestName   request name
     * @param supplier      supplier
     * @param <R>           response
     * @return response of supplier
     */
    public <R> R processRequest(
        final QualifiedName qualifiedName,
        final String requestName,
        final Supplier<R> supplier) {
        /**
         *  TODO: 1. add rate limiter and metrics statics
         *  TODO: 2. add async process
         */

        long time = System.currentTimeMillis();
        try {
            log.info("Handling request: {} for {}", requestName, qualifiedName);
            return supplier.get();
        } catch (SchemaException e) {
            throw e;
        } catch (Throwable e) {
            throw new SchemaException(String.format("%s request failed due to: %s %s", requestName, e.getMessage(), e.getCause()), e);
        }

    }

    /**
     * Request processor to handle request.
     *
     * @param requestName request name
     * @param supplier    supplier
     * @param <R>         response
     * @return response of supplier
     */
    public <R> R processRequest(
        final String requestName,
        final Supplier<R> supplier) {
        long time = System.currentTimeMillis();
        try {
            log.info("Handling request: {}", requestName);
            return supplier.get();
        } catch (SchemaException e) {
            throw e;
        } catch (Throwable e) {
            throw new SchemaException(String.format("Request: %s failed due to %s %s", requestName, e.getMessage(), e.getCause()), e);
        }
    }
}
