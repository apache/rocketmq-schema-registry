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

package org.apache.rocketmq.schema.registry.client.serde.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.config.AvroSerdeConfig;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroDeserializer<T> implements Deserializer<T> {

    Logger log = LoggerFactory.getLogger(AvroDeserializer.class);
    protected SchemaRegistryClient schemaRegistry;
    private boolean useGenericReader;
    private boolean useTargetVersionSchema;
    private long schemaTargetVersion;

    public AvroDeserializer(){}

    public AvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
        schemaRegistry = schemaRegistryClient;
    }

    @Override
    public void configure(Map<String, Object> configs) {
        AvroSerdeConfig config = new AvroSerdeConfig(configs);
        this.useGenericReader = config.useGenericReader();
        this.useTargetVersionSchema = config.useTargetVersionSchema();
        //get schema by version, if didn't configure this, would use the latest version
        this.schemaTargetVersion = config.schemaTargetVersion();
    }

    @Override
    public T deserialize(String subject, byte[] payload) {
        return this.deserialize(subject, payload, null);
    }

    public T deserialize(String subject, byte[] payload, Schema readerSchema)
            throws SerializationException {
        if (schemaRegistry == null) {
            throw new SerializationException("please initialize the schema registry client first");
        }
        if (payload == null) {
            return null;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bais, null);

            GetSchemaResponse response;
            if (useTargetVersionSchema) {
                response = AvroSerdeConfig.SCHEMA_TARGET_VERSION_DEFAULT == schemaTargetVersion
                    ? schemaRegistry.getSchemaBySubject(subject)
                    : schemaRegistry.getSchemaBySubjectAndVersion(subject, schemaTargetVersion);
            } else {
                ByteBuffer buffer = ByteBuffer.allocate(16);
                decoder.readBytes(buffer);
                long schemaRecordId = buffer.getLong();
                response = schemaRegistry.getSchemaByRecordId(subject, schemaRecordId);
            }
            Schema writerSchema = new Schema.Parser().parse(response.getIdl());
            if (readerSchema == null) {
                readerSchema = getReaderSchema(writerSchema);
            }

            DatumReader<T> datumReader = getDatumReader(writerSchema, readerSchema);
            return datumReader.read(null, decoder);
        } catch (RestClientException e) {
            log.warn("get schema by record id failed, maybe the schema storage service not available now", e);
            throw new SerializationException("get schema by record id failed, maybe the schema storage service not available now", e);
        } catch (IOException e) {
            log.warn("deserialize failed", e);
            throw new SerializationException("deserialize error", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Schema getReaderSchema(Schema writerSchema) {
        if (useGenericReader) {
            return writerSchema;
        } else {
            Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
            if (readerClass == null) {
                throw new SerializationException("cannot get a schema for a SpecificRecord");
            }
            try {
                return readerClass.newInstance().getSchema();
            } catch (InstantiationException e) {
                throw new SerializationException("cannot initialize reader schema by writerSchema class", e);
            } catch (IllegalAccessException e) {
                throw new SerializationException("not allowed initialize reader schema by writerSchema class", e);
            }
        }
    }

    private DatumReader<T> getDatumReader(Schema schema, Schema readerSchema) {
        if (useGenericReader) {
            return new GenericDatumReader<>(schema, readerSchema);
        } else {
            return new SpecificDatumReader<>(schema, readerSchema);
        }
    }

    @Override
    public void close() {
    }
}
