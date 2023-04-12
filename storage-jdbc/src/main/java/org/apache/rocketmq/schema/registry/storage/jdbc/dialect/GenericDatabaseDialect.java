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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.storage.jdbc.common.ExpressionBuilder;
import org.apache.rocketmq.schema.registry.storage.jdbc.common.IdentifierRules;
import org.apache.rocketmq.schema.registry.storage.jdbc.common.Operator;
import org.apache.rocketmq.schema.registry.storage.jdbc.configs.JdbcStorageConfigConstants;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class GenericDatabaseDialect implements DatabaseDialect {

    protected static final List<String> FIELDS_DEFAULT =
        Arrays.stream(new String[] {"schema_full_name", "schema_info"}).collect(Collectors.toList());
    protected static final String DDL_FILE = "/%s-storage-ddl.sql";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final int VALIDITY_CHECK_TIMEOUT_S = 5;
    private static int jdbcMajorVersion;
    private final AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
    private final IdentifierRules defaultIdentifierRules = IdentifierRules.DEFAULT;
    private String dbType;
    private String jdbcUrl;
    private String userName;
    private String password;
    private int maxConnectionAttempts;
    private long connectionRetryBackoff;
    private TableId tableId;
    private int count = 0;
    private Connection connection;

    public GenericDatabaseDialect(Properties props, String dbType) throws IOException {
        this.initConfig(props, dbType);
        this.createStorageTables();
    }


    private void initConfig(Properties config, String dbType) {
        // connection info
        this.dbType = dbType;
        this.jdbcUrl = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_URL, null);
        this.userName = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_USER, null);
        this.password = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_PASSWORD, null);
        Assert.notNull(jdbcUrl, "Configuration jdbc url cannot be empty");
        Assert.notNull(userName, "Configuration jdbc userName cannot be empty");
        Assert.notNull(password, "Configuration jdbc password cannot be empty");

        this.maxConnectionAttempts =
            Integer.parseInt(config.getProperty(JdbcStorageConfigConstants.MAX_CONNECTIONS_ATTEMPTS,
                JdbcStorageConfigConstants.MAX_CONNECTIONS_ATTEMPTS_DEFAULT));
        this.connectionRetryBackoff = Long.parseLong(config.getProperty(JdbcStorageConfigConstants.CONNECTION_RETRY_BACKOFF,
            JdbcStorageConfigConstants.CONNECTION_RETRY_BACKOFF_DEFAULT));

        // Storage db and tables
        String database = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_DATABASE_NAME,
            JdbcStorageConfigConstants.DATABASE_DEFAULT);
        String schemaName = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_SCHEMA_NAME,
            null);
        String tableName = config.getProperty(JdbcStorageConfigConstants.STORAGE_JDBC_TABLE_NAME,
            JdbcStorageConfigConstants.TABLE_NAME_DEFAULT);

        this.tableId = new TableId(tableName, database, schemaName);
    }

    @Override
    public String dbType() {
        return dbType;
    }

    /**
     * create storage tables
     */
    protected void createStorageTables() throws IOException {
        InputStream inputStream = GenericDatabaseDialect.class.getResourceAsStream(String.format(DDL_FILE, dbType()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        List<String> ddl = Lists.newArrayList();
        String line;
        while ((line = reader.readLine()) != null) {
            ddl.add(line);
        }
        String[] statements = ddl.stream()
            .map(String::trim)
            .filter(x -> !x.startsWith("--") && !x.isEmpty())
            .map(
                x -> {
                    final Matcher m =
                        COMMENT_PATTERN.matcher(x);
                    return m.matches() ? m.group(1) : x;
                })
            .collect(Collectors.joining("\n"))
            .split(";");

        // create db and tables
        try (Statement st = getConnection().createStatement()) {
            for (String statement : statements) {
                if (tableId.getCatalogName() != null) {
                    statement = statement.replace(TableId.DB_NAME, tableId.getCatalogName().trim());
                }
                if (tableId.getSchemaName() != null) {
                    statement = statement.replace(TableId.SCHEMA_NAME, tableId.getSchemaName().trim());
                }
                statement = statement.replace(TableId.TABLE_NAME, tableId.getTableName().trim());
                st.execute(statement.trim());
            }
        } catch (SQLException sqe) {
            throw new SchemaException("Init database and table is failed", sqe);
        }
    }

    protected ExpressionBuilder expressionBuilder() {
        return identifierRules().expressionBuilder();
    }

    /**
     * Load one data from schema table by schema full name
     *
     * @param keyField
     * @return
     */
    @Override
    public String buildSelectOneStatement(String keyField) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("SELECT ");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(fields());
        builder.append(" from ");
        builder.append(tableId);
        // append where
        builder.append(" WHERE ");
        builder.append(keyField);
        builder.append(" = ?");
        return builder.toString();
    }


    /**
     * Load all data from table
     *
     * @return
     */
    @Override
    public String buildSelectStatement() {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("SELECT ");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(FIELDS_DEFAULT);
        builder.append(" from ");
        builder.append(tableId);
        return builder.toString();
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            if (connection == null) {
                newConnection();
            } else if (!isConnectionValid(connection, VALIDITY_CHECK_TIMEOUT_S)) {
                log.info("The database connection is invalid. Reconnecting...");
                close();
                newConnection();
            }
        } catch (SQLException sqle) {
            throw new SchemaException("Get database is failed", sqle);
        }
        return connection;
    }

    private synchronized void newConnection() throws SQLException {
        int attempts = 0;
        while (attempts < maxConnectionAttempts) {
            try {
                ++count;
                log.info("Attempting to open connection #{}", count);
                connection = createConnection();
                return;
            } catch (SQLException sqle) {
                attempts++;
                if (attempts < maxConnectionAttempts) {
                    log.info("Unable to connect to database on attempt {}/{}. Will retry in {} ms.", attempts,
                        maxConnectionAttempts, connectionRetryBackoff, sqle
                    );
                    try {
                        Thread.sleep(connectionRetryBackoff);
                    } catch (InterruptedException e) {
                        // this is ok because just woke up early
                    }
                } else {
                    throw sqle;
                }
            }
        }
    }

    private Connection createConnection() throws SQLException {
        Properties properties = new Properties();
        if (userName != null) {
            properties.setProperty("user", userName);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        DriverManager.setLoginTimeout(40);
        Connection connection = DriverManager.getConnection(jdbcUrl, properties);
        jdbcMajorVersion = connection.getMetaData().getJDBCMajorVersion();
        return connection;
    }

    /**
     * connection valid
     *
     * @param connection
     * @param timeout
     * @return
     * @throws SQLException
     */
    @Override
    public boolean isConnectionValid(Connection connection, int timeout) throws SQLException {
        if (jdbcMajorVersion >= 4) {
            return connection.isValid(timeout);
        }
        String query = checkConnectionQuery();
        if (query != null) {
            try (Statement statement = connection.createStatement()) {
                if (statement.execute(query)) {
                    ResultSet rs = null;
                    try {
                        rs = statement.getResultSet();
                    } finally {
                        if (rs != null) {
                            rs.close();
                        }
                    }
                }
            }
        }
        return true;
    }

    protected String checkConnectionQuery() {
        return "SELECT 1";
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection db, String query) throws SQLException {
        log.trace("Creating a PreparedStatement '{}'", query);
        PreparedStatement stmt = getConnection().prepareStatement(query);
        return stmt;
    }

    /**
     * build upsert sql
     *
     * @param tableId
     * @param fields
     * @return
     */
    public String buildUpsertStatement(TableId tableId, Collection<String> fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String buildInsertStatement(TableId tableId, Collection<String> columns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(tableId);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(columns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", columns.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildUpdateStatement(
        TableId tableId,
        Collection<String> keyColumns,
        Collection<String> columns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(tableId);
        builder.append(" SET ");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
            .of(columns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        }
        return builder.toString();
    }

    @Override
    public String buildDeleteStatement(
        TableId tableId,
        Collection<String> keyColumns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("DELETE FROM ");
        builder.append(tableId);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        }
        return builder.toString();
    }

    private int bindNoKeyFields(PreparedStatement statement, Collection<String> noKeyValues, int index) throws SQLException {
        for (String value : noKeyValues) {
            statement.setString(index++, value);
        }
        return index;
    }

    private int bindKeyFields(PreparedStatement statement, Collection<String> keyValues, int index) throws SQLException {
        for (String value : keyValues) {
            statement.setString(index++, value);
        }
        return index;
    }

    @Override
    public void bindRecord(PreparedStatement statement, Collection<String> keyValues,
                           Collection<String> noKeyValues, Operator mode) throws SQLException {
        int index = 1;
        switch (mode) {
            case SELECT:
            case INSERT:
            case UPSERT:
                index = bindKeyFields(statement, keyValues, index);
                bindNoKeyFields(statement, noKeyValues, index);
                break;
            case UPDATE:
                index = bindNoKeyFields(statement, noKeyValues, index);
                bindKeyFields(statement, keyValues, index);
                break;
            case DELETE:
                bindKeyFields(statement, keyValues, index);
                break;
        }
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public List<String> fields() {
        return FIELDS_DEFAULT;
    }

    private IdentifierRules identifierRules() {
        if (identifierRules.get() == null) {
            try (Connection connection = getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                String leadingQuoteStr = metaData.getIdentifierQuoteString();
                String trailingQuoteStr = leadingQuoteStr; // JDBC does not distinguish
                String separator = metaData.getCatalogSeparator();
                if (StringUtils.isEmpty(leadingQuoteStr)) {
                    leadingQuoteStr = defaultIdentifierRules.leadingQuoteString();
                    trailingQuoteStr = defaultIdentifierRules.trailingQuoteString();
                }
                if (StringUtils.isEmpty(separator)) {
                    separator = defaultIdentifierRules.identifierDelimiter();
                }
                identifierRules.set(new IdentifierRules(separator, leadingQuoteStr, trailingQuoteStr));
            } catch (SQLException e) {
                if (defaultIdentifierRules != null) {
                    identifierRules.set(defaultIdentifierRules);
                    log.warn("Unable to get identifier metadata; using default rules", e);
                } else {
                    throw new SchemaException("Unable to get identifier metadata", e);
                }
            }
        }
        return identifierRules.get();
    }


    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Ignore errors
            }
        }
    }
}
