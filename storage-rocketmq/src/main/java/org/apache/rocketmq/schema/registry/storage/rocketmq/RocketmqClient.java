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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaExistException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaNotFoundException;
import org.apache.rocketmq.schema.registry.common.json.JsonConverter;
import org.apache.rocketmq.schema.registry.common.json.JsonConverterImpl;
import org.apache.rocketmq.schema.registry.common.model.SchemaDetailInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SubjectInfo;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.DELETE_KEYS;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_LOCAL_CACHE_PATH;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_LOCAL_CACHE_PATH_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_COMPACT_TOPIC_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_CONSUMER_GROUP;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_CONSUMER_GROUP_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_NAMESRV;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_NAMESRV_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_PRODUCER_GROUP;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_PRODUCER_GROUP_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_TOPIC;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_TOPIC_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_USE_COMPACT_TOPIC;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKETMQ_USE_COMPACT_TOPIC_DEFAULT;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKSDB_SCHEMA_COLUMN_FAMILY;
import static org.apache.rocketmq.schema.registry.storage.rocketmq.configs.RocketmqConfigConstants.STORAGE_ROCKSDB_SUBJECT_COLUMN_FAMILY;

@Slf4j
public class RocketmqClient {

    private DefaultMQProducer producer;
    private DefaultLitePullConsumer scheduleConsumer;
    private DefaultMQAdminExt mqAdminExt;
    private String storageTopic;
    private boolean useCompactTopic;
    private String cachePath;
    private JsonConverter converter;
    private final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    private final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> cfHandleMap = new HashMap<>();

    private ScheduledExecutorService scheduledExecutorService;

    private static final Integer PULL_TASK_INTERVAL = 5 * 1000;

    /**
     * RocksDB for cache
     */
    // TODO setCreateMissingColumnFamilies
    private final Options options = new Options().setCreateIfMissing(true);
    private final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
    private RocksDB cache;

    public RocketmqClient(Properties props) {
        init(props);
        createStorageTopic();
        startLocalCache();
        startRemoteStorage();
    }

    private void createStorageTopic() {

        try {
            mqAdminExt.start();

            // check if the topic exists
            TopicRouteData topicRouteData = null;
            try {
                topicRouteData = mqAdminExt.examineTopicRouteInfo(storageTopic);
            } catch (MQClientException e) {
                log.warn("maybe the storage topic not found, need to create");
            } catch (Exception e) {
                throw new SchemaException("Failed to create storage rocketmq topic", e);
            }

            if (topicRouteData != null && CollectionUtils.isNotEmpty(topicRouteData.getBrokerDatas())
                && CollectionUtils.isNotEmpty(topicRouteData.getQueueDatas())) {
                log.info("the storage topic already exist, no need to create");
                return;
            }

            try {
                ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
                HashMap<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
                for (BrokerData brokerData : brokerAddrTable.values()) {
                    TopicConfig topicConfig = new TopicConfig();
                    topicConfig.setTopicName(storageTopic);
                    topicConfig.setReadQueueNums(8);
                    topicConfig.setWriteQueueNums(8);
                    if (useCompactTopic) {
                        Map<String, String> attributes = new HashMap<>(1);
                        attributes.put("+delete.policy", "COMPACTION");
                        topicConfig.setAttributes(attributes);
                    }
                    String brokerAddr = brokerData.selectBrokerAddr();
                    mqAdminExt.createAndUpdateTopicConfig(brokerAddr, topicConfig);
                }
            } catch (Exception e) {
                throw new SchemaException("Failed to create storage rocketmq topic", e);
            } finally {
                mqAdminExt.shutdown();
            }

        } catch (MQClientException e) {
            throw new SchemaException("Rocketmq admin tool start failed", e);
        }

    }

    private void startLocalCache() {
        try {
            CommonUtil.mkdir(cachePath);
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

            scheduleConsumer.setPullThreadNums(4);
            scheduleConsumer.start();

            Collection<MessageQueue> messageQueueList = scheduleConsumer.fetchMessageQueues(storageTopic);
            scheduleConsumer.assign(messageQueueList);
            messageQueueList.forEach(mq -> {
                try {
                    scheduleConsumer.seekToBegin(mq);
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            });
            this.scheduledExecutorService.scheduleAtFixedRate(new RocketmqStoragePullTask(),
                0, PULL_TASK_INTERVAL, TimeUnit.MILLISECONDS);

        } catch (MQClientException e) {
            throw new SchemaException("Rocketmq client start failed", e);
        }
    }

    public class RocketmqStoragePullTask implements Runnable {

        @Override
        public void run() {
            try {
                List<MessageExt> msgList = scheduleConsumer.poll(1000);
                if (CollectionUtils.isNotEmpty(msgList)) {
                    msgList.forEach(this::consumeMessage);
                }
                scheduleConsumer.commitSync();
            } catch (Exception e) {
                log.error("consume message exception, consume offset may not commit");
            }
        }

        private void consumeMessage(MessageExt msg) {
            if (msg.getKeys() == null) {
                return;
            }
            synchronized (this) {
                try {
                    log.info("receive msg, queue={}, offset={}, key={}, the content is {}", msg.getQueueId(),
                        msg.getQueueOffset(), msg.getKeys(), new String(msg.getBody()));
                    byte[] schemaFullName = converter.toBytes(msg.getKeys());
                    byte[] schemaInfoBytes = msg.getBody();
                    SchemaInfo update = converter.fromJson(schemaInfoBytes, SchemaInfo.class);
                    boolean isSchemaDeleted = Boolean.parseBoolean(msg.getUserProperty(DELETE_KEYS));
                    if (isSchemaDeleted) {
                        // delete
                        deleteAllSubject(update);
                        cache.delete(schemaCfHandle(), schemaFullName);
                    }
                    else {
                        byte[] lastRecordBytes = converter.toJsonAsBytes(update.getLastRecord());

                        byte[] result = cache.get(schemaCfHandle(), schemaFullName);
                        if (result == null) {
                            // register
                            cache.put(schemaCfHandle(), schemaFullName, schemaInfoBytes);
                            cache.put(subjectCfHandle(), converter.toBytes(update.subjectFullName()), lastRecordBytes);
                        } else {
                            SchemaInfo current = converter.fromJson(result, SchemaInfo.class);
                            boolean isVersionDeleted = current.getRecordCount() > update.getRecordCount();
                            if (current.getLastModifiedTime() != null && update.getLastModifiedTime() != null &&
                                current.getLastModifiedTime().after(update.getLastModifiedTime())) {
                                log.info("Current Schema is later version, no need to update.");
                                return;
                            }
                            if (current.getLastRecordVersion() == update.getLastRecordVersion() && !isVersionDeleted) {
                                log.info("Schema version is the same, no need to update.");
                                return;
                            }
                            if (current.getLastRecordVersion() > update.getLastRecordVersion() && !isVersionDeleted) {
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
                    log.error("Update schema cache failed, msg {}", new String(msg.getBody()), e);
                    throw new SchemaException("Update schema " + msg.getKeys() + " failed.", e);
                }
            }
        }
    }

    // TODO: next query on other machine may can't found schema in cache since send is async with receive
    public SchemaInfo registerSchema(SchemaInfo schema) {
        byte[] schemaFullName = converter.toBytes(schema.schemaFullName());
        byte[] schemaInfo = converter.toJsonAsBytes(schema);

        try {
            synchronized (this) {
                if (cache.get(schemaCfHandle(), schemaFullName) != null) {
                    throw new SchemaExistException(schema.getQualifiedName());
                }

                Message message = new Message(storageTopic, "", schema.schemaFullName(), schemaInfo);
                SendResult result = sendOrderMessageToRocketmq(message);
                if (!result.getSendStatus().equals(SendStatus.SEND_OK)) {
                    throw new SchemaException("Register schema: " + schema.getQualifiedName() + " failed: " + result.getSendStatus());
                }
            }

            return schema;
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("register schema failed", e);
        }
    }

    public void deleteBySubject(QualifiedName name) {

        SchemaInfo schemaInfo = getSchemaInfoBySubject(name.subjectFullName());
        if (schemaInfo == null) {
            throw new SchemaNotFoundException(name);
        }

        try {
            synchronized (this) {
                schemaInfo.setLastModifiedTime(new Date());
                schemaInfo.setDetails(new SchemaDetailInfo());
                Message msg = new Message(storageTopic, "", schemaInfo.schemaFullName(), converter.toJsonAsBytes(schemaInfo));
                msg.putUserProperty(DELETE_KEYS, "true");
                SendResult result = sendOrderMessageToRocketmq(msg);
                if (!result.getSendStatus().equals(SendStatus.SEND_OK)) {
                    throw new SchemaException("Delete schema: " + name + " failed: " + result.getSendStatus());
                }
            }
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("Delete schema " + name + " failed", e);
        }
    }

    public void deleteByVersion(QualifiedName name) {

        SchemaInfo schemaInfo = getSchemaInfoBySubject(name.subjectFullName());
        if (schemaInfo == null || schemaInfo.getDetails() == null || schemaInfo.getDetails().getSchemaRecords() == null) {
            throw new SchemaNotFoundException(name);
        }
        List<SubjectInfo> subjects = schemaInfo.getLastRecord().getSubjects();
        List<SchemaRecordInfo> schemaRecords = schemaInfo.getDetails().getSchemaRecords();
        schemaRecords.removeIf(record -> record.getVersion() == name.getVersion());
        if (CollectionUtils.isEmpty(schemaRecords)) {
            deleteBySubject(name);
        }
        // delete but still need bind subject
        if (schemaInfo.getLastRecord().getSubjects().isEmpty()) {
            schemaInfo.getLastRecord().setSubjects(subjects);
        }
        byte[] schemaInfoBytes = converter.toJsonAsBytes(schemaInfo);

        try {
            synchronized (this) {
                Message msg = new Message(storageTopic, "", schemaInfo.schemaFullName(), schemaInfoBytes);
                SendResult result = sendOrderMessageToRocketmq(msg);
                if (result.getSendStatus() != SendStatus.SEND_OK) {
                    throw new SchemaException("Update " + name + " failed: " + result.getSendStatus());
                }
            }
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("Update schema " + name + " failed", e);
        }
    }

    public SchemaInfo updateSchema(SchemaInfo update) {
        byte[] schemaInfo = converter.toJsonAsBytes(update);

        try {
            synchronized (this) {
                Message msg = new Message(storageTopic, "", update.schemaFullName(), schemaInfo);
                SendResult result = sendOrderMessageToRocketmq(msg);
                if (result.getSendStatus() != SendStatus.SEND_OK) {
                    throw new SchemaException("Update " + update.getQualifiedName() + " failed: " + result.getSendStatus());
                }
            }
            return update;
        } catch (SchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaException("Update schema " + update.getQualifiedName() + " failed", e);
        }
    }

    public byte[] getSchema(String schemaFullName) {
        try {
            // TODO: get from rocketmq topic if cache not contain
            return cache.get(schemaCfHandle(), converter.toBytes(schemaFullName));
        } catch (RocksDBException e) {
            throw new SchemaException("Get schema " + schemaFullName + " failed", e);
        }
    }

    public byte[] getBySubject(String subjectFullName) {
        try {
            return cache.get(subjectCfHandle(), converter.toBytes(subjectFullName));
        } catch (RocksDBException e) {
            throw new SchemaException("Get by subject " + subjectFullName + " failed", e);
        }
    }

    public SchemaInfo getSchemaInfoBySubject(String subjectFullName) {
        byte[] lastRecordBytes = getBySubject(subjectFullName);
        if (lastRecordBytes == null) {
            return null;
        }
        SchemaRecordInfo lastRecord = converter.fromJson(lastRecordBytes, SchemaRecordInfo.class);
        byte[] result = getSchema(lastRecord.getSchema());
        return result == null ? null : converter.fromJson(result, SchemaInfo.class);
    }

    public List<String> getSubjects(StorageServiceContext context, String tenant) {
        List<String> subjects = new ArrayList<>();
        RocksIterator iterator = cache.newIterator(subjectCfHandle());
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String subjectFullName = new String(iterator.key());
            String[] subjectFromCache = subjectFullName.split("/");
            String tenantFromKey = subjectFromCache[1];
            String subjectFromKey = subjectFromCache[2];
            if (isSuperAdmin(context.getUserName()) || tenant.equals(tenantFromKey)) {
                subjects.add(subjectFromKey);
            }
        }
        return subjects;
    }

    private boolean isSuperAdmin(String userName) {
        // check superAdmin
        return false;
    }

    private void init(Properties props) {
        this.useCompactTopic = Boolean.parseBoolean(props.getProperty(STORAGE_ROCKETMQ_USE_COMPACT_TOPIC,
            STORAGE_ROCKETMQ_USE_COMPACT_TOPIC_DEFAULT));
        String defaultTopic = useCompactTopic ? STORAGE_ROCKETMQ_COMPACT_TOPIC_DEFAULT : STORAGE_ROCKETMQ_TOPIC_DEFAULT;
        this.storageTopic = props.getProperty(STORAGE_ROCKETMQ_TOPIC, defaultTopic);
        this.cachePath = props.getProperty(STORAGE_LOCAL_CACHE_PATH, STORAGE_LOCAL_CACHE_PATH_DEFAULT);

        this.producer = new DefaultMQProducer(
            props.getProperty(STORAGE_ROCKETMQ_PRODUCER_GROUP, STORAGE_ROCKETMQ_PRODUCER_GROUP_DEFAULT)
        );

        this.producer.setNamesrvAddr(
            props.getProperty(STORAGE_ROCKETMQ_NAMESRV, STORAGE_ROCKETMQ_NAMESRV_DEFAULT)
        );

        this.scheduleConsumer = new DefaultLitePullConsumer(
            props.getProperty(STORAGE_ROCKETMQ_CONSUMER_GROUP, STORAGE_ROCKETMQ_CONSUMER_GROUP_DEFAULT)
        );

        this.scheduleConsumer.setNamesrvAddr(
            props.getProperty(STORAGE_ROCKETMQ_NAMESRV, STORAGE_ROCKETMQ_NAMESRV_DEFAULT)
        );

        this.mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(
            props.getProperty(STORAGE_ROCKETMQ_NAMESRV, STORAGE_ROCKETMQ_NAMESRV_DEFAULT)
        );

        this.converter = new JsonConverterImpl();

        this.scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RocketmqStoragePullTask"));
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

    private SendResult sendOrderMessageToRocketmq(Message msg) throws Exception {
        return this.producer.send(msg, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message message, Object shardingKey) {
                int select = Math.abs(shardingKey.hashCode());
                if (select < 0) {
                    select = 0;
                }
                return mqs.get(select % mqs.size());
            }
        }, msg.getKeys());
    }
}
