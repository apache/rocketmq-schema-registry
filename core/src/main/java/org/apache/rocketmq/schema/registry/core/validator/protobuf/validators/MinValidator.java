
package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ValidationConditions;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.Validator;

import java.util.Map;

public class MinValidator implements Validator {
	@Override
	public void validate(GeneratedMessageV3 protoMessage, FieldDescriptor fieldDescriptor, Object fieldValue, Map.Entry<FieldDescriptor, Object> rule)
			throws NumberFormatException, MessageValidationException {
		String extensionValueStr = rule.getValue().toString();
		String fieldValueStr = fieldValue.toString();
		if (fieldValue instanceof Long) {
			ValidationConditions.checkRule(Long.valueOf(extensionValueStr) <= Long.valueOf(fieldValueStr), protoMessage, fieldDescriptor, fieldValue, rule);
		}
		if (fieldValue instanceof Integer) {
			ValidationConditions.checkRule(Integer.valueOf(extensionValueStr) <= Integer.valueOf(fieldValueStr), protoMessage, fieldDescriptor, fieldValue, rule);
		}
		if (fieldValue instanceof Float) {
			ValidationConditions.checkRule(Float.valueOf(extensionValueStr) <= Float.valueOf(fieldValueStr), protoMessage, fieldDescriptor, fieldValue, rule);
		}
		if (fieldValue instanceof Double) {
			ValidationConditions.checkRule(Double.valueOf(extensionValueStr) <= Double.valueOf(fieldValueStr), protoMessage, fieldDescriptor, fieldValue, rule);
		}
	}

}
