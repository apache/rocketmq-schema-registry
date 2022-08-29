package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.NumberMaxMin;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NumberMaxMinTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testNumberInside() {
		NumberMaxMin b = NumberMaxMin.newBuilder().setAge(20).build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}

	@Test
	public void testNumberTooHigh() {
		NumberMaxMin b = NumberMaxMin.newBuilder().setAge(110).build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testNumberTooLow() {
		NumberMaxMin b = NumberMaxMin.newBuilder().setAge(10).build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testNumberMax() {
		NumberMaxMin b = NumberMaxMin.newBuilder().setAge(100).build();
		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);

	}

	@Test
	public void testNumberMin() {
		NumberMaxMin b = NumberMaxMin.newBuilder().setAge(18).build();
		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);

	}
}
