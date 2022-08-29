package org.apache.rocketmq.schema.registry.core.validator.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

import java.util.List;
import java.util.Map;

public class ProtobufValidator {
	
	private ValidatorRegistry validatorRegistry;
	
	/**
	 * @param validatorRegistry The {@link ValidatorRegistry} which should be used for validation.
	 */
	public ProtobufValidator(ValidatorRegistry validatorRegistry) {
		this.validatorRegistry = validatorRegistry;
	}
	
	/**
	 * The default constructor which uses the global {@link ValidatorRegistry}.
	 */
	public ProtobufValidator() {
		this(ValidatorRegistry.globalValidatorRegistry());
	}
	
	private void doValidate(GeneratedMessageV3 message, Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue, DescriptorProtos.FieldOptions options)
			throws IllegalArgumentException, MessageValidationException {
		for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : options.getAllFields().entrySet()) {
			try {
				validatorRegistry.getValidator(entry.getKey()).validate(message, fieldDescriptor, fieldValue, entry);
			} catch(UnsupportedOperationException e) {
				// Add more info and rethrow
				throw new UnsupportedOperationException("Error validating field "+fieldDescriptor+ " with value "+fieldValue+ " and rule "+entry+ " due to "+e.getMessage(),e);
			}
		}
	}
	
	/**
	 * @param protoMessage The protobuf message object to validate
	 * @throws MessageValidationException Further information about the failed field
	 */
	@SuppressWarnings("unchecked")
	public void validate(GeneratedMessageV3 protoMessage) throws MessageValidationException {
		for (Descriptors.FieldDescriptor fieldDescriptor : protoMessage.getDescriptorForType().getFields()) {
			
			Object fieldValue;
			if (fieldDescriptor.isRepeated()) {
				fieldValue = protoMessage.getField(fieldDescriptor);
				//validate array of messages recursively
				if (fieldValue instanceof List) {
					for (Object subMessage : (List<Object>) fieldValue) {
						if (subMessage instanceof GeneratedMessageV3) {
							validate((GeneratedMessageV3) subMessage);
						}
					}
				}
			} else {
				fieldValue = protoMessage.hasField(fieldDescriptor) ? protoMessage.getField(fieldDescriptor) : null;
				//validate recursively the message
				if (fieldValue instanceof GeneratedMessageV3)
					validate((GeneratedMessageV3) fieldValue);
			}
			
			//validate options on the field
			if (fieldDescriptor.getOptions().getAllFields().size() > 0) {
				doValidate(protoMessage, fieldDescriptor, fieldValue, fieldDescriptor.getOptions());
			}
		}
	}
	
}
