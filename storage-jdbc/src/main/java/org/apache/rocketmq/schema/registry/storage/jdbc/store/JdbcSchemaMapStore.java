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

package org.apache.rocketmq.schema.registry.storage.jdbc.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.json.JsonConverter;
import org.apache.rocketmq.schema.registry.common.json.JsonConverterImpl;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.storage.jdbc.common.Operator;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.DatabaseDialectProvider;
import org.apache.rocketmq.schema.registry.storage.jdbc.dialect.DiscoverDialectFactory;
import org.springframework.util.Assert;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.rocketmq.schema.registry.storage.jdbc.configs.JdbcStorageConfigConstants.STORAGE_JDBC_TYPE;
import static org.apache.rocketmq.schema.registry.storage.jdbc.configs.JdbcStorageConfigConstants.STORAGE_JDBC_TYPE_MYSQL;

/**
 * Jdbc map store
 */
@Slf4j
public class JdbcSchemaMapStore implements IMapStore<String, SchemaInfo> {

    public final static String SCHEMAS = "schemas";
    private DatabaseDialect dialect;
    private JsonConverter converter;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties config, String mapKey) {
        Assert.isTrue(mapKey.equals(SCHEMAS), "Schema map key should be [schemas]");
        String type = config.getProperty(STORAGE_JDBC_TYPE, STORAGE_JDBC_TYPE_MYSQL);
        DatabaseDialectProvider provider = DiscoverDialectFactory.getDialectProvider(type);
        try {
            this.dialect = provider.createDialect(config);
        } catch (IOException e) {
            throw new SchemaException("Create jdbc storage instance failed.", e);
        }
        this.converter = new JsonConverterImpl();
    }

    @Override
    public void store(String s, SchemaInfo schemaInfo) {
        try (PreparedStatement ps = dialect.createPreparedStatement(
            dialect.getConnection(),
            dialect.buildUpsertStatement(dialect.tableId(), dialect.fields()))
        ) {
            List<String> keyFieldValues = Lists.newArrayList(schemaInfo.schemaFullName());
            List<String> noKeyFieldValues = Lists.newArrayList(converter.toString(schemaInfo));
            dialect.bindRecord(ps, keyFieldValues, noKeyFieldValues, Operator.UPSERT);
            ps.executeUpdate();
        } catch (SQLException sqe) {
            throw new SchemaException("Registry schema handler", sqe);
        }
    }

    @Override
    public void storeAll(Map<String, SchemaInfo> map) {
        Set<Map.Entry<String, SchemaInfo>> schemaInfos = map.entrySet();
        for (Map.Entry<String, SchemaInfo> schemaInfoEntry : schemaInfos) {
            store(schemaInfoEntry.getKey(), schemaInfoEntry.getValue());
        }
    }

    @Override
    public void delete(String key) {
        try (PreparedStatement statement = dialect.createPreparedStatement(
                dialect.getConnection(),
                dialect.buildDeleteStatement(dialect.tableId(), Lists.newArrayList(dialect.fields().get(0))))) {
            dialect.bindRecord(statement, Lists.newArrayList(key), null, Operator.DELETE);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAll(Collection<String> collection) {
        Iterator<String> iterator = collection.iterator();
        while (iterator.hasNext()) {
            delete(iterator.next());
        }
    }

    @Override
    public SchemaInfo load(String key) {
        try (PreparedStatement preparedStatement =
                     dialect.createPreparedStatement(
                             dialect.getConnection(),
                             dialect.buildSelectOneStatement(dialect.fields().stream().findFirst().get()))
        ) {
            dialect.bindRecord(preparedStatement, Lists.newArrayList(), Lists.newArrayList(key), Operator.SELECT);
            Map<String, SchemaInfo> schemaInfoMap = refresh(preparedStatement);
            if (schemaInfoMap.isEmpty()) {
                return null;
            }
            return schemaInfoMap.entrySet().stream().findFirst().get().getValue();
        } catch (SQLException | JsonProcessingException sqe) {
            throw new SchemaException("", sqe);
        }
    }

    @Override
    public Map<String, SchemaInfo> loadAll(Collection<String> collection) {
        Map<String, SchemaInfo> schemaInfos = Maps.newConcurrentMap();
        Iterator<String> iterator = collection.iterator();
        while (iterator.hasNext()) {
            String schemaKey = iterator.next();
            schemaInfos.put(schemaKey, load(schemaKey));
        }
        return schemaInfos;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        try (PreparedStatement preparedStatement =
                     dialect.createPreparedStatement(
                             dialect.getConnection(),
                             dialect.buildSelectStatement()
                     )
        ) {
            Map<String, SchemaInfo> schemaInfoMap = refresh(preparedStatement);
            return schemaInfoMap.keySet();
        } catch (SQLException | JsonProcessingException sqe) {
            log.error("Load all schema thought jdbc is failed", sqe);
            throw new SchemaException("Load all schema key is failed", sqe);
        }
    }

    private Map<String, SchemaInfo> refresh(PreparedStatement preparedStatement) throws SQLException, JsonProcessingException {
        Map<String, SchemaInfo> schemaInfos = Maps.newConcurrentMap();
        ResultSet result = preparedStatement.executeQuery();
        while (result.next()) {
            String schemaInfo = result.getString(2);
            schemaInfos.put(
                result.getString(1),
                converter.fromJson(schemaInfo, SchemaInfo.class)
            );
        }
        return schemaInfos;
    }

    @Override
    public void destroy() {
        if (this.dialect != null) {
            this.dialect.close();
        }
    }
}
