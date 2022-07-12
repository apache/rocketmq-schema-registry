/**
 * Copyright 2022, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package org.apache.rocketmq.schema.registry.core.api;

import java.util.function.Supplier;

import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

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
     * @param supplier supplier
     * @param <R> response
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
        } finally {
            log.info("Handle request: {} cost {}", requestName, (System.currentTimeMillis() - time));
        }
    }

    /**
     * Request processor to handle request.
     *
     * @param requestName   request name
     * @param supplier supplier
     * @param <R> response
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
        }  catch (Throwable e) {
            throw new SchemaException(String.format("Request: %s failed due to %s %s", requestName, e.getMessage(), e.getCause()), e);
        } finally {
            log.info("Handle request: {} cost {}", requestName, (System.currentTimeMillis() - time));
        }
    }
}
