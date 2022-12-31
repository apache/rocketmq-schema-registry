package org.apache.rocketmq.schema.registry.core.validator.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.core.provider.diff.SchemaDiff;
import org.apache.rocketmq.schema.registry.core.schema.ProtobufSchema;

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
	 * @param update The protobuf update message object
	 * @param current The protobuf message object to validate
	 * @throws MessageValidationException Further information about the failed field
	 */
	@SuppressWarnings("unchecked")
	public void validate(ProtobufSchema update, List<ProtobufSchema> current) throws MessageValidationException {
		for (ProtobufSchema currentItem: current) {
			SchemaDiff.compare(update, currentItem);
		}
	}
	
}
