package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.Forbidden;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ForbiddenRuleTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testRuleFail() {
		Forbidden b = Forbidden.newBuilder().setName("0").build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRulePass() {
		Forbidden b = Forbidden.newBuilder().build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}
}
