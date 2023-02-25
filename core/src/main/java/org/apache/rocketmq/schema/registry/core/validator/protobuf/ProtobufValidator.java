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
