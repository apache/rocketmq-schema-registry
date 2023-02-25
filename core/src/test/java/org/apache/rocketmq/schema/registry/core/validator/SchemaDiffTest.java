package org.apache.rocketmq.schema.registry.core.validator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.rocketmq.schema.registry.core.schema.ProtobufSchema;
import org.apache.rocketmq.schema.registry.core.schema.ProtobufSchemaUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaDiffTest {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	
	private static final String recordSchemaString = "syntax = \"proto3\";\n"
			+ "\n"
			+ "option java_package = \"protobuf.test\";\n"
			+ "option java_outer_classname = \"TestMessageProtos\";\n"
			+ "\n"
			+ "import \"google/protobuf/descriptor.proto\";\n"
			+ "\n"
			+ "message TestMessage {\n"
			+ "    string test_string = 1 [json_name = \"test_str\"];\n"
			+ "    bool test_bool = 2;\n"
			+ "    bytes test_bytes = 3;\n"
			+ "    double test_double = 4;\n"
			+ "    float test_float = 5;\n"
			+ "    fixed32 test_fixed32 = 6;\n"
			+ "    fixed64 test_fixed64 = 7;\n"
			+ "    int32 test_int32 = 8;\n"
			+ "    int64 test_int64 = 9;\n"
			+ "    sfixed32 test_sfixed32 = 10;\n"
			+ "    sfixed64 test_sfixed64 = 11;\n"
			+ "    sint32 test_sint32 = 12;\n"
			+ "    sint64 test_sint64 = 13;\n"
			+ "    uint32 test_uint32 = 14;\n"
			+ "    uint64 test_uint64 = 15;\n"
			+ "}\n";
	
	private static final ProtobufSchema recordSchema = new ProtobufSchema(recordSchemaString);
	
	@Test
	public void testRecordToProtobuf() throws Exception {
		String json = "{\n"
				+ "    \"test_string\": \"string\",\n"
				+ "    \"test_bool\": true,\n"
				+ "    \"test_bytes\": \"aGVsbG8=\",\n"
				// base-64 encoded "hello"
				+ "    \"test_double\": 800.25,\n"
				+ "    \"test_float\": 23.4,\n"
				+ "    \"test_fixed32\": 32,\n"
				+ "    \"test_fixed64\": 64,\n"
				+ "    \"test_int32\": 32,\n"
				+ "    \"test_int64\": 64,\n"
				+ "    \"test_sfixed32\": 32,\n"
				+ "    \"test_sfixed64\": 64,\n"
				+ "    \"test_sint32\": 32,\n"
				+ "    \"test_sint64\": 64,\n"
				+ "    \"test_uint32\": 32,\n"
				+ "    \"test_uint64\": 64\n"
				+ "}";
		
		Object result = ProtobufSchemaUtils.toObject(jsonTree(json), recordSchema);
		assertTrue(result instanceof DynamicMessage);
		DynamicMessage resultRecord = (DynamicMessage) result;
		Descriptors.Descriptor desc = resultRecord.getDescriptorForType();
		Descriptors.FieldDescriptor fd = desc.findFieldByName("test_string");
		assertEquals("string", resultRecord.getField(fd));
		fd = desc.findFieldByName("test_bool");
		assertEquals(true, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_bytes");
		assertEquals("hello", ((ByteString) resultRecord.getField(fd)).toStringUtf8());
		fd = desc.findFieldByName("test_double");
		assertEquals(800.25, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_float");
		assertEquals(23.4f, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_fixed32");
		assertEquals(32, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_fixed64");
		assertEquals(64L, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_int32");
		assertEquals(32, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_int64");
		assertEquals(64L, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_sfixed32");
		assertEquals(32, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_sfixed64");
		assertEquals(64L, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_sint32");
		assertEquals(32, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_sint64");
		assertEquals(64L, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_uint32");
		assertEquals(32, resultRecord.getField(fd));
		fd = desc.findFieldByName("test_uint64");
		assertEquals(64L, resultRecord.getField(fd));
	}
	private static JsonNode jsonTree(String jsonData) {
		try {
			return objectMapper.readTree(jsonData);
		} catch (Exception e) {
			throw new RuntimeException("Failed to parse JSON", e);
		}
	}
}
