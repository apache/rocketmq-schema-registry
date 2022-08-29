package org.apache.rocketmq.schema.registry.core.validator.protobuf;

import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.validators.*;

import java.util.Map;

public class ValidatorRegistry {
	
	private static ValidatorRegistry globalValidatorRegistry = createDefaultRegistry();
	
	private Map<Descriptors.FieldDescriptor, Validator> validatorMap;
	
	/**
	 * Constructor which builds a {@link ValidatorRegistry} with predefined validators from the validatorMap.
	 */
	public ValidatorRegistry(Map<Descriptors.FieldDescriptor, Validator> validatorMap) {
		this.validatorMap = validatorMap;
	}
	
	/**
	 * The default constructor which builds a empty {@link ValidatorRegistry}.
	 */
	public ValidatorRegistry() {
		this(Maps.newConcurrentMap());
	}
	
	/**
	 * @param descriptor The descriptor for which the {@link Validator} should be removed.
	 * @return {@link Validator} The validator for a {@link com.google.protobuf.Descriptors.FieldDescriptor} or
	 *         null if none is registered.
	 */
	public Validator getValidator(Descriptors.FieldDescriptor descriptor) {
		return validatorMap.get(descriptor);
	}
	
	/**
	 * @return The globally shared {@link ValidatorRegistry}.
	 */
	public static ValidatorRegistry globalValidatorRegistry() {
		return globalValidatorRegistry;
	}
	
	/**
	 * @return A {@link ValidatorRegistry} with all built-in validators.
	 */
	public static ValidatorRegistry createDefaultRegistry() {
		
		ValidatorRegistry validatorRegistry = new ValidatorRegistry();
		
		Map<Descriptors.FieldDescriptor, Validator> validatorMap = validatorRegistry.validatorMap;
		validatorMap.put(Validation.max.getDescriptor(), new MaxValidator());
		validatorMap.put(Validation.min.getDescriptor(), new MinValidator());
		validatorMap.put(Validation.repeatMax.getDescriptor(), new RepeatMaxValidator());
		validatorMap.put(Validation.repeatMin.getDescriptor(), new RepeatMinValidator());
		validatorMap.put(Validation.future.getDescriptor(), new FutureValidator());
		validatorMap.put(Validation.past.getDescriptor(), new PastValidator());
		validatorMap.put(Validation.regex.getDescriptor(), new RegexValidator());
		validatorMap.put(Validation.required.getDescriptor(), new RequiredValidator());
		validatorMap.put(Validation.forbidden.getDescriptor(), new ForbiddenValidator());
		
		return validatorRegistry;
	}
}
