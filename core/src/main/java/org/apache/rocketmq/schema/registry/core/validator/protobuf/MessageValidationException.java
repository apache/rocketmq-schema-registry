package org.apache.rocketmq.schema.registry.core.validator.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Map;

public class MessageValidationException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	private static String computeMessage(
			GeneratedMessageV3 message, Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue,
			Map.Entry<Descriptors.FieldDescriptor, Object> validationRule) {
		StringBuilder b = new StringBuilder();
		b.append(fieldDescriptor.getFile().getName());
		b.append('/');
		b.append(fieldDescriptor.getFullName());
		b.append(", rule ");
		String validationRuleString;
		if (validationRule.getValue() == null || "".equals(validationRule.getValue())) {
			validationRuleString = validationRule.getKey().toString();
		} else {
			validationRuleString = validationRule.toString();
		}
		
		b.append(validationRuleString);
		if (null != fieldValue && !"".equals(fieldValue)) {
			b.append(", invalid field value is ");
			b.append(fieldValue);
		}
		return b.toString();
	}
	
	private String fieldName;
	
	private Object fieldValue;
	
	private String protoName;
	
	private Object validationRule;
	
	public MessageValidationException(GeneratedMessageV3 message, Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue,
			Map.Entry<Descriptors.FieldDescriptor, Object> validationRule) {
		super(computeMessage(message, fieldDescriptor, fieldValue, validationRule));
		this.protoName = fieldDescriptor.getFile().getName();
		this.fieldName = fieldDescriptor.getFullName();
		this.fieldValue = fieldValue;
		this.validationRule = validationRule;
	}
	
	public Object getFieldValue() {
		return fieldValue;
	}
	
}

