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

package org.apache.rocketmq.schema.registry.common.context;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RequestContext implements Serializable {
    private static final long serialVersionUID = 1772214628830653791L;

    public static final String UNKNOWN = "UNKNOWN";
    private final long timestamp = System.currentTimeMillis();
    private final String id = UUID.randomUUID().toString();
    private String userName;
    private final String clientAppName;
    private final String clientId;
    private final String apiUri;
    private final String scheme;
    private final String token;

    public RequestContext() {
        this.userName = null;
        this.clientAppName = null;
        this.clientId = null;
        this.apiUri = UNKNOWN;
        this.scheme = UNKNOWN;
        this.token = null;
    }

    protected RequestContext(
        @Nullable final String userName,
        @Nullable final String clientAppName,
        @Nullable final String clientId,
        final String apiUri,
        final String scheme,
        final String token
    ) {
        this.userName = userName;
        this.clientAppName = clientAppName;
        this.clientId = clientId;
        this.apiUri = apiUri;
        this.scheme = scheme;
        this.token = token;
    }

    @Override
    public String toString() {
        return "RequestContext{" +
            "timestamp=" + timestamp +
            ", id='" + id + '\'' +
            ", userName='" + userName + '\'' +
            ", clientAppName='" + clientAppName + '\'' +
            ", clientId='" + clientId + '\'' +
            ", apiUri='" + apiUri + '\'' +
            ", scheme='" + scheme + '\'' +
            ", metaAccount='" + token + '\'' +
            '}';
    }
}
