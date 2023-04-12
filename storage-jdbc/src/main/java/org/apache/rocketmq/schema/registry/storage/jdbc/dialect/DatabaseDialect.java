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

import org.apache.rocketmq.schema.registry.storage.jdbc.common.Operator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

/**
 * database dialect
 */
public interface DatabaseDialect extends ConnectionProvider {
    String dbType();

    TableId tableId();

    List<String> fields();

    PreparedStatement createPreparedStatement(Connection db, String sql) throws SQLException;

    String buildSelectStatement();

    String buildSelectOneStatement(String keyField);

    String buildUpsertStatement(TableId tableId, Collection<String> fields);

    String buildInsertStatement(TableId tableId, Collection<String> columns);

    String buildUpdateStatement(TableId tableId, Collection<String> keyColumns, Collection<String> columns);

    String buildDeleteStatement(TableId tableId, Collection<String> keyColumns);

    void bindRecord(PreparedStatement statement, Collection<String> keyValues,
                    Collection<String> noKeyValues, Operator mode) throws SQLException;

}
