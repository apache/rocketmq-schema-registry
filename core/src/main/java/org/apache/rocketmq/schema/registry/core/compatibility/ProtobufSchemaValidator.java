package org.apache.rocketmq.schema.registry.core.compatibility;

import com.google.protobuf.*;
import org.apache.avro.SchemaValidationException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaCompatibilityException;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.core.schema.ProtobufSchema;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.schema.registry.common.model.Compatibility.*;
import static org.apache.rocketmq.schema.registry.common.model.Compatibility.FULL_TRANSITIVE;

public class ProtobufSchemaValidator implements org.apache.rocketmq.schema.registry.core.compatibility.SchemaValidator {
	
	private static final Map<Compatibility, ProtobufValidator> SCHEMA_VALIDATOR_CACHE = new HashMap<>(6);
	
	{
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(BACKWARD,
				new ProtobufValidator());
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(BACKWARD_TRANSITIVE,
				new ProtobufValidator());
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(FORWARD,
				new ProtobufValidator());
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(FORWARD_TRANSITIVE,
				new ProtobufValidator());
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(FULL,
				new ProtobufValidator());
		SCHEMA_VALIDATOR_CACHE.putIfAbsent(FULL_TRANSITIVE,
				new ProtobufValidator());
	}
	
	
	@Override public void validate(SchemaInfo update, SchemaInfo current) {
		ProtobufValidator validator =
				SCHEMA_VALIDATOR_CACHE.get(current.getMeta().getCompatibility());
		ProtobufSchema updateSchema = (ProtobufSchema) update;
		try {
			List<ProtobufSchema> existing = new ArrayList<>();
			for (String schemaIdl : current.getAllRecordIdlInOrder()) {
				ProtobufSchema currentItem = new ProtobufSchema(schemaIdl);
				existing.add(currentItem);
			}
			validator.validate(updateSchema, existing);
			
		} catch ( MessageValidationException e) {
			throw new SchemaCompatibilityException("Schema compatibility validation failed", e);
		}
	}
	
}
