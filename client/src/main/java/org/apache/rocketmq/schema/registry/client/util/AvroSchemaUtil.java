package org.apache.rocketmq.schema.registry.client.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.rocketmq.schema.registry.client.avro.AvroSchema;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AvroSchemaUtil {

    private static final EncoderFactory encoderFactory = EncoderFactory.get();
    private static final DecoderFactory decoderFactory = DecoderFactory.get();
    private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;
    private static final ObjectMapper jsonMapperWithOrderedProps =
            JsonMapper.builder()
                    .nodeFactory(new SortingNodeFactory(false))
                    .build();
    static class SortingNodeFactory extends JsonNodeFactory {
        public SortingNodeFactory(boolean bigDecimalExact) {
            super(bigDecimalExact);
        }

        @Override
        public ObjectNode objectNode() {
            return new ObjectNode(this, new TreeMap<>());
        }
    }

    private static int DEFAULT_CACHE_CAPACITY = 1000;

    private static final Map<String, Schema> primitiveSchemas;
    private static final Map<Schema, Schema> transformedSchemas =
            new ConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);

    static {
        primitiveSchemas = new HashMap<>();
        primitiveSchemas.put("Null", createPrimitiveSchema("null"));
        primitiveSchemas.put("Boolean", createPrimitiveSchema("boolean"));
        primitiveSchemas.put("Integer", createPrimitiveSchema("int"));
        primitiveSchemas.put("Long", createPrimitiveSchema("long"));
        primitiveSchemas.put("Float", createPrimitiveSchema("float"));
        primitiveSchemas.put("Double", createPrimitiveSchema("double"));
        primitiveSchemas.put("String", createPrimitiveSchema("string"));
        primitiveSchemas.put("Bytes", createPrimitiveSchema("bytes"));
    }

    private static Schema createPrimitiveSchema(String type) {
        String schemaString = String.format("{\"type\" : \"%s\"}", type);
        return new AvroSchema(schemaString).getSchema();
    }

    public static Map<String, Schema> getPrimitiveSchemas() {
        return Collections.unmodifiableMap(primitiveSchemas);
    }

    public static Schema getSchema(Object object) {
        return getSchema(object, false, false, false);
    }

    public static Schema getSchema(Object object, boolean useReflection,
                                   boolean reflectionAllowNull, boolean removeJavaProperties) {
        if (object == null) {
            return primitiveSchemas.get("Null");
        } else if (object instanceof Boolean) {
            return primitiveSchemas.get("Boolean");
        } else if (object instanceof Integer) {
            return primitiveSchemas.get("Integer");
        } else if (object instanceof Long) {
            return primitiveSchemas.get("Long");
        } else if (object instanceof Float) {
            return primitiveSchemas.get("Float");
        } else if (object instanceof Double) {
            return primitiveSchemas.get("Double");
        } else if (object instanceof CharSequence) {
            return primitiveSchemas.get("String");
        } else if (object instanceof byte[] || object instanceof ByteBuffer) {
            return primitiveSchemas.get("Bytes");
        } else if (useReflection) {
            Schema schema = reflectionAllowNull ? ReflectData.AllowNull.get().getSchema(object.getClass())
                    : ReflectData.get().getSchema(object.getClass());
            if (schema == null) {
                throw new SerializationException("Schema is null for object of class " + object.getClass()
                        .getCanonicalName());
            } else {
                return schema;
            }
        } else if (object instanceof GenericContainer) {
            Schema schema = ((GenericContainer) object).getSchema();
            if (removeJavaProperties) {
                final Schema s = schema;
                schema = transformedSchemas.computeIfAbsent(s, k -> removeJavaProperties(s));
            }
            return schema;
        } else if (object instanceof Map) {
            // This case is unusual -- the schema isn't available directly anywhere, instead we have to
            // take get the value schema out of one of the entries and then construct the full schema.
            Map mapValue = ((Map) object);
            if (mapValue.isEmpty()) {
                // In this case the value schema doesn't matter since there is no content anyway. This
                // only works because we know in this case that we are only using this for conversion and
                // no data will be added to the map.
                return Schema.createMap(primitiveSchemas.get("Null"));
            }
            Schema valueSchema = getSchema(mapValue.values().iterator().next());
            return Schema.createMap(valueSchema);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
                            + "Float, Double, String, byte[] and IndexedRecord");
        }
    }

    private static Schema removeJavaProperties(Schema schema) {
        try {
            JsonNode node = jsonMapper.readTree(schema.toString());
            removeProperty(node, "avro.java.string");
            AvroSchema avroSchema = new AvroSchema(node.toString());
            return avroSchema.getSchema();
        } catch (IOException e) {
            throw new SerializationException("Could not parse schema: " + schema.toString());
        }
    }

    private static void removeProperty(JsonNode node, String propertyName) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.remove(propertyName);
            Iterator<JsonNode> elements = objectNode.elements();
            while (elements.hasNext()) {
                removeProperty(elements.next(), propertyName);
            }
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            Iterator<JsonNode> elements = arrayNode.elements();
            while (elements.hasNext()) {
                removeProperty(elements.next(), propertyName);
            }
        }
    }
}
