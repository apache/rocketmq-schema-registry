package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.Required;
import validation.RequiredRepeated;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RequiredRuleTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testRuleFail() {
		Required b = Required.newBuilder().build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRulePass() {
		Required b = Required.newBuilder().setName("Required").build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}

	@Test
	public void testEmptyString() {
		Required b = Required.newBuilder().setName("").build();

		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);
	}

	@Test
	public void testRepeatedFieldPass() {
		RequiredRepeated b = RequiredRepeated.newBuilder().addName("").build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}

	@Test
	public void testRepeatedFieldFail() {
		RequiredRepeated b = RequiredRepeated.newBuilder().build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

}
