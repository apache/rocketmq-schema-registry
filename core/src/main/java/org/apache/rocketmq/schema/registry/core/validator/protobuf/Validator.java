package org.apache.rocketmq.schema.registry.core.validator.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Map;

public interface Validator {
	
	void validate(GeneratedMessageV3 protoMessage, Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue, Map.Entry<Descriptors.FieldDescriptor, Object> extensionValue)
			throws MessageValidationException;
}
