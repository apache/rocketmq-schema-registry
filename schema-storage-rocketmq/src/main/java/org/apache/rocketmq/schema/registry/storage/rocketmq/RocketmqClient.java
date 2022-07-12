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

package org.apache.rocketmq.schema.registry.storage.rocketmq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaExistException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaNotFoundException;
import org.apache.rocketmq.schema.registry.common.json.JsonConverter;
import org.apache.rocketmq.schema.registry.common.json.JsonConverterImpl;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SubjectInfo;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.DELETE_KEYS;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_LOCAL_CACHE_PATH;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_LOCAL_CACHE_PATH_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_CONSUMER_GROUP;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_CONSUMER_GROUP_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_NAMESRV;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_NAMESRV_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_PRODUCER_GROUP;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_PRODUCER_GROUP_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_TOPIC;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_TOPIC_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKSDB_SCHEMA_COLUMN_FAMILY;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKSDB_SUBJECT_COLUMN_FAMILY;

@Slf4j
public class RocketmqClient {

    private Properties properties;
    private DefaultMQProducer producer;
    private DefaultMQPushConsumer scheduleConsumer;
    private String storageTopic;
    private String cachePath;
    private JsonConverter converter;
    private final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    private final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> cfHandleMap = new HashMap<>();


    /**
     * RocksDB for cache
     */
    // TODO setCreateMissingColumnFamilies
    private final Options options = new Options().setCreateIfMissing(true);
    private final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
    private RocksDB cache;

    public RocketmqClient(Properties props) {
        init(props);
        startRemoteStorage();
        startLocalCache();
    }

    private void startLocalCache() {
        try {
            List<byte[]> cfs = RocksDB.listColumnFamilies(options, cachePath);
            if (cfs.size() <= 1) {
                List<byte[]> columnFamilies = Arrays.asList(STORAGE_ROCKSDB_SCHEMA_COLUMN_FAMILY,
                    STORAGE_ROCKSDB_SUBJECT_COLUMN_FAMILY);
                // TODO: add default cf in handles when needed
                cache = org.rocksdb.RocksDB.open(options, cachePath);
                cfDescriptors.addAll(columnFamilies.stream()
                    .map(ColumnFamilyDescriptor::new)
                    .collect(Collectors.toList()));
                cfHandleList.addAll(cache.createColumnFamilies(cfDescriptors));
            } else {
                cfDescriptors.addAll(cfs.stream()
                    .map(ColumnFamilyDescriptor::new)
                    .collect(Collectors.toList()));
                cache = org.rocksdb.RocksDB.open(dbOptions, cachePath, cfDescriptors, cfHandleList);
            }

            cfHandleMap.putAll(
                cfHandleList.stream().collect(Collectors.toMap(h -> {
                    try {
                        return new String(h.getName());
                    } catch (RocksDBException e) {
                        throw new SchemaException("Failed to open RocksDB", e);
                    }
                }, h -> h)));

            assert cfHandleList.size() >= 2;
        } catch (RocksDBException e) {
            throw new SchemaException("Failed to open RocksDB", e);
        }
    }

    public void startRemoteStorage() {
        try {
            producer.start();

            scheduleConsumer.subscribe(storageTopic, "*");
            scheduleConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    msgs.forEach(msg -> {
                        synchronized (this) {
                            try {
                                if (msg.getKeys().equals(DELETE_KEYS)) {
                                    // delete
                                    byte[] schemaFullName = msg.getBody();
                                    byte[] schemaInfoBytes = cache.get(schemaCfHandle(), schemaFullName);
                                    if (schemaInfoBytes != null) {
                                        deleteAllSubject(converter.fromJson(schemaInfoBytes, SchemaInfo.class));
                                        cache.delete(schemaCfHandle(), schemaFullName);
                                    }
                                } else {
                                    byte[] schemaFullName = converter.toBytes(msg.getKeys());
                                    byte[] schemaInfoBytes = msg.getBody();
                                    SchemaInfo update = converter.fromJson(schemaInfoBytes, SchemaInfo.class);
                                    byte[] lastRecordBytes = converter.toJsonAsBytes(update.getLastRecord());

                                    byte[] result = cache.get(schemaCfHandle(), schemaFullName);
                                    if (result == null) {
                                        // register
                                        cache.put(schemaCfHandle(), schemaFullName, schemaInfoBytes);
                                        cache.put(subjectCfHandle(), converter.toBytes(update.subjectFullName()), lastRecordBytes);
                                    } else {
                                        SchemaInfo current = converter.fromJson(result, SchemaInfo.class);
                                        if (current.getLastRecordVersion() == update.getLastRecordVersion()) {
                                            return;
                                        }
                                        if (current.getLastRecordVersion() > update.getLastRecordVersion()) {
                                            throw new SchemaException("Schema version is invalid, update: "
                                                + update.getLastRecordVersion() + ", but current: " + current.getLastRecordVersion());
                                        }

                                        cache.put(schemaCfHandle(), schemaFullName, schemaInfoBytes);
                                        update.getLastRecord().getSubjects().forEach(subject -> {
                                            try {
                                                cache.put(subjectCfHandle(), converter.toBytes(subject.fullName()), lastRecordBytes);
                                            } catch (RocksDBException e) {
                                                throw new SchemaException("Update schema: " + update.getQualifiedName() + " failed.", e);
                                            }
                                        });
                                    }
                                }
                            } catch (Throwable e) {
                                throw new SchemaException("Rebuild schema cache failed", e);
                            }
                        }
                    });
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            scheduleConsumer.start();
        } catch (MQClientException e) {
            throw new SchemaException("Rocketmq client start failed", e);
        }
    }

    // TODO: next query on other machine may can't found schema in cache since send is async with receive
    public SchemaInfo registerSchema(SchemaInfo schema) {
        byte[] subjectFullName = converter.toBytes(schema.subjectFullName());
        byte[] schemaFullName = converter.toBytes(schema.schemaFullName());
        byte[] schemaInfo = converter.toJsonAsBytes(schema);
        byte[] lastRecord = converter.toJsonAsBytes(schema.getLastRecord());

        try {
            synchronized (this) {
                if (cache.get(schemaCfHandle(), schemaFullName) != null) {
                    throw new SchemaExistException(schema.getQualifiedName());
                }

                SendResult result = producer.send(new Message(storageTopic, "", schema.schemaFullName(), schemaInfo));
                if (!result.getSendStatus().equals(SendStatus.SEND_OK)) {
                    throw new SchemaException("Register schema: " + schema.getQualifiedName() + " failed: " + result.getSendStatus());
                }

                cache.put(schemaCfHandle(), schemaFullName, schemaInfo);
                cache.put(subjectCfHandle(), subjectFullName, lastRecord);
            }

            return schema;
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("register schema failed", e);
        }
    }

    public void delete(QualifiedName name) {
        byte[] schemaFullName = converter.toBytes(name.schemaFullName());

        try {
            synchronized (this) {
                byte[] schemaInfoBytes = cache.get(schemaCfHandle(), schemaFullName);
                if (schemaInfoBytes == null) {
                    throw new SchemaNotFoundException(name);
                }

                Message msg = new Message(storageTopic, "", DELETE_KEYS, schemaFullName);
                SendResult result = producer.send(msg);
                if (!result.getSendStatus().equals(SendStatus.SEND_OK)) {
                    throw new SchemaException("Delete schema: " + name + " failed: " + result.getSendStatus());
                }

                cache.delete(schemaCfHandle(), schemaFullName);
                deleteAllSubject(converter.fromJson(schemaInfoBytes, SchemaInfo.class));
            }
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("Delete schema " + name + " failed", e);
        }
    }

    public SchemaInfo updateSchema(SchemaInfo update) {
        byte[] schemaFullName = converter.toBytes(update.schemaFullName());
        byte[] schemaInfo = converter.toJsonAsBytes(update);
        byte[] lastRecord = converter.toJsonAsBytes(update.getLastRecord());

        try {
            synchronized (this) {
                Message msg = new Message(storageTopic, "", update.schemaFullName(), schemaInfo);
                SendResult result = producer.send(msg);
                if (result.getSendStatus() != SendStatus.SEND_OK) {
                    throw new SchemaException("Update " + update.getQualifiedName() + " failed: " + result.getSendStatus());
                }

                cache.put(schemaCfHandle(), schemaFullName, schemaInfo);
                update.getLastRecord().getSubjects().forEach(subject -> {
                    try {
                        cache.put(subjectCfHandle(), converter.toBytes(subject.fullName()), lastRecord);
                    } catch (RocksDBException e) {
                        throw new SchemaException("Update schema: " + update.getQualifiedName() + " failed", e);
                    }
                });
            }
            return update;
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("Update schema " + update.getQualifiedName() + " failed", e);
        }
    }

    public byte[] getSchema(QualifiedName qualifiedName) {
        try {
            // TODO: get from rocketmq topic if cache not contain
            return cache.get(schemaCfHandle(), converter.toBytes(qualifiedName.schemaFullName()));
        } catch (RocksDBException e) {
            throw new SchemaException("Get schema " + qualifiedName + " failed", e);
        }
    }

    public byte[] getBySubject(QualifiedName qualifiedName) {
        try {
            return cache.get(subjectCfHandle(), converter.toBytes(qualifiedName.subjectFullName()));
        } catch (RocksDBException e) {
            throw new SchemaException("Get by subject " + qualifiedName + " failed", e);
        }
    }

    private void init(Properties props) {
        this.properties = props;
        this.storageTopic = props.getProperty(STORAGE_ROCKETMQ_TOPIC, STORAGE_ROCKETMQ_TOPIC_DEFAULT);
        this.cachePath = props.getProperty(STORAGE_LOCAL_CACHE_PATH, STORAGE_LOCAL_CACHE_PATH_DEFAULT);

        this.producer = new DefaultMQProducer(
            props.getProperty(STORAGE_ROCKETMQ_PRODUCER_GROUP, STORAGE_ROCKETMQ_PRODUCER_GROUP_DEFAULT)
        );

        this.producer.setNamesrvAddr(
            props.getProperty(STORAGE_ROCKETMQ_NAMESRV, STORAGE_ROCKETMQ_NAMESRV_DEFAULT)
        );

        this.scheduleConsumer = new DefaultMQPushConsumer(
            props.getProperty(STORAGE_ROCKETMQ_CONSUMER_GROUP, STORAGE_ROCKETMQ_CONSUMER_GROUP_DEFAULT)
        );

        this.scheduleConsumer.setNamesrvAddr(
            props.getProperty(STORAGE_ROCKETMQ_NAMESRV, STORAGE_ROCKETMQ_NAMESRV_DEFAULT)
        );

        this.converter = new JsonConverterImpl();
    }

    private ColumnFamilyHandle schemaCfHandle() {
        return cfHandleMap.get(new String(STORAGE_ROCKSDB_SCHEMA_COLUMN_FAMILY));
    }

    private ColumnFamilyHandle subjectCfHandle() {
        return cfHandleMap.get(new String(STORAGE_ROCKSDB_SUBJECT_COLUMN_FAMILY));
    }

    private void deleteAllSubject(SchemaInfo current) {
        // delete subjects bind to any version
        List<SchemaRecordInfo> allSchemaRecords = current.getDetails().getSchemaRecords();
        List<String> allSubjects = allSchemaRecords.parallelStream()
            .flatMap(record -> record.getSubjects().stream().map(SubjectInfo::fullName))
            .collect(Collectors.toList());

        allSubjects.forEach(subject -> {
            try {
                cache.delete(subjectCfHandle(), converter.toBytes(subject));
            } catch (RocksDBException e) {
                throw new SchemaException("Delete schema " + current.getQualifiedName() + "'s subjects failed", e);
            }
        });
    }
}
