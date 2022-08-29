package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.Past;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PastRuleTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testRuleFail() {
		Past b = Past.newBuilder().setPastTimemilles(System.currentTimeMillis() + 10000).build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRulePass() {
		Past b = Past.newBuilder().setPastTimemilles(System.currentTimeMillis() - 10000).build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}
}
