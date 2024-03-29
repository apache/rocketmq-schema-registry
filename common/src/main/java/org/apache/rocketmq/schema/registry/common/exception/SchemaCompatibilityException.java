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
import org.apache.rocketmq.schema.registry.common.QualifiedName;

@Getter
public class SchemaCompatibilityException extends SchemaException {
    private static final long serialVersionUID = 2602020608319903212L;

    private final int errorCode = 40901;

    public SchemaCompatibilityException(final QualifiedName qualifiedName) {
        this(String.format("Schema: %s validate failed.", qualifiedName));
    }

    public SchemaCompatibilityException(final String msg) {
        super(msg);
    }

    public SchemaCompatibilityException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
