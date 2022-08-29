package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.RegexMatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RegexRuleTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testRuleFail() {
		RegexMatch b = RegexMatch.newBuilder().setName("00").build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRulePass() {
		RegexMatch b = RegexMatch.newBuilder().setName("AB").build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}

	@Test
	public void testNull() {
		RegexMatch b = RegexMatch.newBuilder().build();

		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);
	}
}
