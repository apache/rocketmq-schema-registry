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

package org.apache.rocketmq.schema.registry.storage.jdbc.dialect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.rocketmq.schema.registry.storage.jdbc.common.IdentifierRules;

@Data
@AllArgsConstructor
@Builder
public class TableId {
    public static final String DB_NAME = "#DBNAME#";
    public static final String SCHEMA_NAME = "#SCHEMANAME#";
    public static final String TABLE_NAME = "#TABLENAME#";
    private String tableName;
    private String catalogName;
    private String schemaName;

    private void appendTo(StringBuilder builder, String name, String quote) {
        builder.append(name.trim()).append(quote);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (catalogName != null) {
            appendTo(builder, catalogName.trim(), IdentifierRules.DEFAULT_ID_DELIM);
        }
        if (schemaName != null) {
            appendTo(builder, schemaName.trim(), IdentifierRules.DEFAULT_ID_DELIM);
        }
        builder.append(tableName.trim());
        return builder.toString();
    }
}
