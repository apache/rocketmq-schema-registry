
package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ValidationConditions;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.Validator;

import java.util.Map;

public class FutureValidator implements Validator {
	@Override
	public void validate(GeneratedMessageV3 protoMessage, FieldDescriptor fieldDescriptor, Object fieldValue, Map.Entry<FieldDescriptor, Object> rule)
			throws MessageValidationException {
		Boolean extensionValueBoolean = (Boolean) rule.getValue();
		Long fieldValueLong = (Long) fieldValue;
		if (extensionValueBoolean) {
			long now = System.currentTimeMillis();
			ValidationConditions.checkRule(now < fieldValueLong, protoMessage, fieldDescriptor, fieldValue, rule);
		}
	}
}
