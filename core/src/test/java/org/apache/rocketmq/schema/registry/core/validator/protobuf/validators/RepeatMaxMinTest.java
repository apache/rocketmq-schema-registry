package org.apache.rocketmq.schema.registry.core.validator.protobuf.validators;

import org.apache.rocketmq.schema.registry.core.validator.protobuf.MessageValidationException;
import org.apache.rocketmq.schema.registry.core.validator.protobuf.ProtobufValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import validation.RepeatMaxMin;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RepeatMaxMinTest {

	private ProtobufValidator validator = new ProtobufValidator();

	@Test
	public void testRepeatInside() {
		RepeatMaxMin b = RepeatMaxMin.newBuilder().addHobbies("Football").addHobbies("Golf").addHobbies("Swimming").build();

		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);
	}

	@Test
	public void testRepeatTooHigh() {
		RepeatMaxMin b = RepeatMaxMin.newBuilder().addHobbies("Football").addHobbies("Golf").addHobbies("Swimming").addHobbies("Skiing").addHobbies("Riding")
				.addHobbies("Flying").build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRepeatTooLow() {
		RepeatMaxMin b = RepeatMaxMin.newBuilder().addHobbies("Football").build();
		Executable e = () -> validator.validate(b);
		assertThrows(MessageValidationException.class, e);

	}

	@Test
	public void testRepeatMaxHigh() {
		RepeatMaxMin b = RepeatMaxMin.newBuilder().addHobbies("Football").addHobbies("Swimming").addHobbies("Skiing").addHobbies("Riding")
				.addHobbies("Flying").build();
		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);

	}

	@Test
	public void testRepeatMinLow() {
		RepeatMaxMin b = RepeatMaxMin.newBuilder().addHobbies("Football").addHobbies("Yoga").build();
		Executable e = () -> validator.validate(b);
		assertDoesNotThrow(e);

	}
}
