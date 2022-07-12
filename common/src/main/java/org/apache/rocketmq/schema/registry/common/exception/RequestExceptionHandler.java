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

package org.apache.rocketmq.schema.registry.common.exception;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import lombok.extern.slf4j.Slf4j;

@RestControllerAdvice
@Slf4j
public class RequestExceptionHandler {

    /**
     * Handle Schema service Exceptions.
     *
     * @param response The HTTP response
     * @param e        The inner exception to handle
     * @throws IOException on error in sending error
     */
    @ExceptionHandler({SchemaException.class})
    public void handleException(
        final HttpServletResponse response,
        final SchemaException e
    ) throws IOException {
        final int status;

        if (e instanceof SchemaNotFoundException) {
            status = HttpStatus.NOT_FOUND.value();
        } else  {
            status = HttpStatus.INTERNAL_SERVER_ERROR.value();
        }

        log.error("Global handle SchemaException: " + e.getMessage(), e);
        response.sendError(status, e.getMessage());
    }

}
