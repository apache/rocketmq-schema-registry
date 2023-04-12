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

package org.apache.rocketmq.schema.registry.storage.jdbc.dialect.mysql;

import org.apache.rocketmq.schema.registry.storage.jdbc.common.ExpressionBuilder;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.DatabaseDialectProvider;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.GenericDatabaseDialect;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.TableId;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;


/**
 * Mysql dialect
 */
public class MysqlDatabaseDialect extends GenericDatabaseDialect {

    public MysqlDatabaseDialect(Properties props, String dbType) throws IOException {
        super(props, dbType);
    }

    @Override
    public String buildUpsertStatement(TableId tableId, Collection<String> fields) {
        final ExpressionBuilder.Transform<String> transform = (builder, col) -> {
            builder.appendColumnName(col);
            builder.append("=values(");
            builder.appendColumnName(col);
            builder.append(")");
        };
        ExpressionBuilder builder = expressionBuilder();
        builder.append("insert into ");
        builder.append(tableId);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(fields);
        builder.append(") values(");
        builder.appendMultiple(",", "?", fields.size());
        builder.append(") on duplicate key update ");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(fields);
        return builder.toString();
    }


    public static class Provider implements DatabaseDialectProvider {

        @Override
        public DatabaseDialect createDialect(Properties config) throws IOException {
            return new MysqlDatabaseDialect(config, databaseType());
        }

        @Override
        public String databaseType() {
            return "mysql";
        }
    }

}
