
package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ValidationConditions;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.Validator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class RegexValidator implements Validator {
	
	private static final Map<String,Pattern> patternCache = new ConcurrentHashMap<>();
	
	@Override
	public void validate(GeneratedMessageV3 protoMessage, FieldDescriptor fieldDescriptor, Object fieldValue, Map.Entry<FieldDescriptor, Object> rule)
			throws MessageValidationException {
		
		String regex = rule.getValue().toString();
		
		Pattern pattern = patternCache.get(regex);
		if(pattern == null) {
			pattern = Pattern.compile(regex);
			patternCache.put(regex, pattern);
		}
		
		String fieldValueStr = (fieldValue == null? "": fieldValue.toString());
		ValidationConditions.checkRule(pattern.matcher(fieldValueStr).matches(), protoMessage, fieldDescriptor, fieldValue, rule);
	}
}
