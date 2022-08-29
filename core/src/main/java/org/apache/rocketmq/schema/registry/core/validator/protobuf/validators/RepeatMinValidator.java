
package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ValidationConditions;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.Validator;

import java.util.Collection;
import java.util.Map;

public class RepeatMinValidator implements Validator {
	@Override
	public void validate(GeneratedMessageV3 protoMessage, FieldDescriptor fieldDescriptor, Object fieldValue, Map.Entry<FieldDescriptor, Object> rule)
			throws MessageValidationException {
		Collection<?> fieldValueCol = (Collection<?>) fieldValue;
		ValidationConditions.checkRule(fieldValueCol.size() >= (Long) rule.getValue(), protoMessage, fieldDescriptor, fieldValue, rule);
	}
}
