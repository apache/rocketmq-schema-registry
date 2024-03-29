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

import lombok.Getter;

@Getter
public class SchemaAuthorizedException extends SchemaException {
    private static final long serialVersionUID = 204882338833006991L;

    private final int errorCode = 40101;

    public SchemaAuthorizedException(final String tenant, final String schemaName) {
        this(String.format("Schema: %s/%s not found, please check your configuration.", tenant, schemaName));
    }

    public SchemaAuthorizedException(final String msg) {
        super(msg);
    }

    public SchemaAuthorizedException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
