/*
 * Copyright 2017-2025 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.avro.model.AvroSchema;
import io.micronaut.avro.model.AvroSchema.Field;
import io.micronaut.avro.model.AvroSchema.LogicalType;
import io.micronaut.avro.model.AvroSchema.Type;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.core.type.Argument;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.Decoder;
import io.micronaut.serde.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro Implementation of {@link Decoder}
 * This class provides methods for decoding data into Avro format.
 *
 * @author Ali Linaboui
 * @since 1.0
 */
public class AvroSerdeDecoder implements Decoder {

    int fieldIndex = -1;
    private final AvroSchema avroSchema;
    private final org.apache.avro.io.Decoder delegate;
    private final ResourceLoader resourceLoader;

    // Stack to track nested array contexts
    private final Deque<ArrayContext> arrayContextStack = new ArrayDeque<>();

    public AvroSerdeDecoder(org.apache.avro.io.Decoder delegate, ResourceLoader resourceLoader) {
        this(delegate, resourceLoader, null);
    }

    private AvroSerdeDecoder(org.apache.avro.io.Decoder delegate, ResourceLoader resourceLoader, AvroSchema schema) {
        this.delegate = delegate;
        this.avroSchema = schema;
        this.resourceLoader = resourceLoader;
    }

    @Override
    public @NonNull Decoder decodeArray(Argument<?> type) throws IOException {
        // If we're already in an array context, we're dealing with a nested array
        if (!arrayContextStack.isEmpty()) {
            ArrayContext parentContext = arrayContextStack.peek();

            // Consume one item from the parent array (this represents the inner array we're about to decode)
            consumeArrayItem();

            // Get the item type from the parent array's item type
            Object itemType = null;
            if (parentContext.itemType instanceof Map<?, ?> map) {
                if ("array".equals(map.get("type"))) {
                    itemType = map.get("items");
                }
            }

            long itemCount = delegate.readArrayStart();
            arrayContextStack.push(new ArrayContext(itemCount, itemType));
            return this;
        }

        // Top-level array
        Object fieldType = getFieldType(avroSchema, fieldIndex);
        if (fieldType instanceof Map<?, ?> map) {
            if ("array".equals(map.get("type"))) {
                // Get the item type for the array
                Object itemType = map.get("items");

                // Read array block count
                long itemCount = delegate.readArrayStart();
                arrayContextStack.push(new ArrayContext(itemCount, itemType));
                return this;
            }
        }
        throw new IllegalStateException("Expected array type but got: " + fieldType);
    }

    @Override
    public boolean hasNextArrayValue() throws IOException {
        if (arrayContextStack.isEmpty()) {
            throw new IllegalStateException("Not in array context");
        }

        ArrayContext currentContext = arrayContextStack.peek();
        if (currentContext.itemsRemaining > 0) {
            return true;
        }
        delegate.arrayNext();
        return false;
    }

    @Override
    public @NonNull Decoder decodeObject(@NonNull Argument<?> type) throws IOException {
        Class<?> targetClass = type.getType();
        if (targetClass.isAnnotationPresent(AvroSchemaSource.class)) {
            String schemaLocation = targetClass.getAnnotation(AvroSchemaSource.class).value();
            try (InputStream stream = resourceLoader.getResourceAsStream(schemaLocation).orElseThrow(() ->
                new IOException("Schema location " + schemaLocation + " not found"))) {
                AvroSchema avroSchema = ObjectMapper.getDefault().readValue(stream, AvroSchema.class);
                return new AvroSerdeDecoder(delegate, resourceLoader, avroSchema);
            }
        }
        throw new IllegalStateException("No schema found for " + targetClass);
    }

    @Override
    public @Nullable String decodeKey() throws IOException {
        fieldIndex++;
        if (fieldIndex < 0 || fieldIndex >= avroSchema.getFields().size()) {
            throw new IllegalStateException("No field index found for " + avroSchema);
        }
        return avroSchema.getFields().get(fieldIndex).getName();
    }

    @Override
    public @NonNull String decodeString() throws IOException {
        // If we're in an array context, handle the item type appropriately
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            return handleString(itemType);
        }
        // Regular field decoding
        Object type = getFieldType(avroSchema, fieldIndex);
        return handleString(type);
    }

    @Override
    public boolean decodeBoolean() throws IOException {
        return decodePrimitive("boolean", Type.BOOLEAN, delegate::readBoolean);
    }

    @Override
    public byte decodeByte() throws IOException {
        return decodePrimitive("int", Type.INT, () -> (byte) delegate.readInt());
    }

    @Override
    public short decodeShort() throws IOException {
        return decodePrimitive("int", Type.INT, () -> (short) delegate.readInt());
    }

    @Override
    public char decodeChar() throws IOException {
        return decodePrimitive("int", Type.INT, () -> (char) delegate.readInt());
    }

    @Override
    public int decodeInt() throws IOException {
        return decodePrimitive("int", Type.INT, delegate::readInt);
    }

    @Override
    public long decodeLong() throws IOException {
        return decodePrimitive("long", Type.LONG, delegate::readLong);
    }

    @Override
    public float decodeFloat() throws IOException {
        return decodePrimitive("float", Type.FLOAT, delegate::readFloat);
    }

    @Override
    public double decodeDouble() throws IOException {
        return decodePrimitive("double", Type.DOUBLE, delegate::readDouble);
    }

    @Override
    public @NonNull BigInteger decodeBigInteger() throws IOException {
        ByteBuffer buffer = readBigIntegerBytes();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new BigInteger(bytes);
    }

    private ByteBuffer readBigIntegerBytes() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            return readBigIntegerBytes(itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        return readBigIntegerBytes(type);
    }

    private ByteBuffer readBigIntegerBytes(Object type) throws IOException {
        if ("bytes".equals(type) || (type instanceof Map<?, ?> map && "bytes".equals(map.get("type")))) {
            return delegate.readBytes(null);
        }
        throw new IllegalStateException("Expected BigInteger type but got: " + type);
    }

    @Override
    public @NonNull BigDecimal decodeBigDecimal() throws IOException {
        String value = readDecimalValue();
        return new BigDecimal(value);
    }

    private String readDecimalValue() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            return readDecimalValue(itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        return readDecimalValue(type);
    }

    private String readDecimalValue(Object type) throws IOException {
        if (type.equals("bytes") || (type instanceof Map<?, ?> fieldType && Type.BYTES == Type.fromString(fieldType.get("type").toString()) &&
            LogicalType.BIG_DECIMAL == LogicalType.fromString(fieldType.get("logicalType").toString()))) {
            return delegate.readString();
        }
        throw new IllegalStateException("Expected decimal type but got: " + type);
    }

    @Override
    public boolean decodeNull() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("null".equals(context.itemType)) {
                consumeArrayItem(); // Only consume the array item if it's null
                return true;
            }
            return false;
        }

        AvroSchema.Field field = avroSchema.getFields().get(fieldIndex);
        return Type.NULL == field.getType();
    }

    @Override
    public @Nullable Object decodeArbitrary() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            return decodeValue(itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        return decodeValue(type);
    }

    @Override
    public @NonNull JsonNode decodeNode() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            return decodeNode(itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        return decodeNode(type);
    }

    private JsonNode decodeNode(Object type) throws IOException {
        Object value = decodeValue(type);
        return convertToJsonNode(value);
    }

    private JsonNode convertToJsonNode(Object value) {
        if (value == null) {
            return JsonNode.nullNode();
        } else if (value instanceof Boolean bool) {
            return JsonNode.createBooleanNode(bool);
        } else if (value instanceof Integer integer) {
            return JsonNode.createNumberNode(integer);
        } else if (value instanceof Long longValue) {
            return JsonNode.createNumberNode(longValue);
        } else if (value instanceof Float floatValue) {
            return JsonNode.createNumberNode(floatValue);
        } else if (value instanceof Double doubleValue) {
            return JsonNode.createNumberNode(doubleValue);
        } else if (value instanceof String string) {
            return JsonNode.createStringNode(string);
        } else if (value instanceof byte[] bytes) {
            return JsonNode.createStringNode(Base64.getEncoder().encodeToString(bytes));
        } else if (value instanceof Map<?, ?> map) {
            Map<String, JsonNode> objectMap = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    objectMap.put(key, convertToJsonNode(entry.getValue()));
                } else {
                    throw new UnsupportedOperationException("Unsupported key type: " + entry.getKey().getClass());
                }
            }
            return JsonNode.createObjectNode(objectMap);
        } else if (value instanceof List<?> list) {
            List<JsonNode> arrayList = new ArrayList<>();
            for (Object item : list) {
                arrayList.add(convertToJsonNode(item));
            }
            return JsonNode.createArrayNode(arrayList);
        }
        throw new UnsupportedOperationException("Unsupported value type: " + value.getClass());
    }

    private Object decodeValue(Object type) throws IOException {
        if (type instanceof String fieldType) {
            switch (Type.fromString(fieldType)) {
                case NULL -> {
                    return null;
                }
                case BOOLEAN -> {
                    return delegate.readBoolean();
                }
                case INT -> {
                    return delegate.readInt();
                }
                case LONG -> {
                    return delegate.readLong();
                }
                case FLOAT -> {
                    return delegate.readFloat();
                }
                case DOUBLE -> {
                    return delegate.readDouble();
                }
                case STRING -> {
                    return delegate.readString();
                }
                case BYTES -> {
                    ByteBuffer buffer = delegate.readBytes(null);
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    return bytes; // Return the byte array directly
                }
                default -> throw new UnsupportedOperationException("Unsupported type: " + fieldType);
            }
        } else if (type instanceof Map<?, ?> fieldTypeMap) {
            String fieldTypeName = (String) fieldTypeMap.get("type");
            switch (Type.fromString(fieldTypeName)) {
                case RECORD -> {
                    Map<String, Object> record = new HashMap<>();
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> fields = (List<Map<String, Object>>) fieldTypeMap.get("fields");
                    for (Map<String, Object> field : fields) {
                        String fieldName = (String) field.get("name");
                        Object fieldType = field.get("type");
                        record.put(fieldName, decodeValue(fieldType));
                    }
                    return record;
                }
                case ARRAY -> {
                    List<Object> list = new ArrayList<>();
                    Object itemType = fieldTypeMap.get("items");
                    for (long count = delegate.readArrayStart(); count != 0; count = delegate.arrayNext()) {
                        for (long i = 0; i < count; i++) {
                            list.add(decodeValue(itemType));
                        }
                    }
                    return list;
                }
                case ENUM -> {
                    int index = delegate.readEnum();
                    @SuppressWarnings("unchecked")
                    List<String> symbols = (List<String>) fieldTypeMap.get("symbols");
                    return symbols.get(index);
                }
                default -> throw new UnsupportedOperationException("Unsupported type: " + fieldTypeName);
            }
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    @Override
    public Decoder decodeBuffer() throws IOException {
        // Buffer the current value into a byte array
        byte[] bufferedData = bufferCurrentValue();
        org.apache.avro.io.Decoder bufferedDecoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(bufferedData, null);

        return new AvroSerdeDecoder(bufferedDecoder, resourceLoader, avroSchema);
    }

    /**
     * Buffers the current value into a byte array.
     * This method reads the current value and serializes it into bytes.
     */
    private byte[] bufferCurrentValue() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder delegate = org.apache.avro.io.EncoderFactory.get().binaryEncoder(outputStream, null);

        Object currentType;
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            currentType = context.itemType;
        } else {
            currentType = getFieldType(avroSchema, fieldIndex);
        }
        bufferValueByType(currentType, delegate);

        return outputStream.toByteArray();
    }

    /**
     * Buffers a value of the specified type using the given encoder.
     */
    private void bufferValueByType(Object type, org.apache.avro.io.Encoder encoder) throws IOException {
        if (type instanceof String fieldType) {
            switch (Type.fromString(fieldType)) {
                case NULL -> {
                    // Null values don't need to be written
                }
                case BOOLEAN -> {
                    encoder.writeBoolean(delegate.readBoolean());
                }
                case INT -> {
                    encoder.writeInt(delegate.readInt());
                }
                case LONG -> {
                    encoder.writeLong(delegate.readLong());
                }
                case FLOAT -> {
                    encoder.writeFloat(delegate.readFloat());
                }
                case DOUBLE -> {
                    encoder.writeDouble(delegate.readDouble());
                }
                case STRING -> {
                    encoder.writeString(delegate.readString());
                }
                case BYTES -> {
                    ByteBuffer buffer = delegate.readBytes(null);
                    byte[] bytes = new byte[buffer.remaining()];
                    encoder.writeBytes(bytes);
                }
                default -> throw new UnsupportedOperationException("Unsupported primitive type: " + fieldType);
            }
        } else if (type instanceof Map<?, ?> fieldTypeMap) {
            String fieldTypeName = (String) fieldTypeMap.get("type");
            switch (Type.fromString(fieldTypeName)) {
                case RECORD -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> fields = (List<Map<String, Object>>) fieldTypeMap.get("fields");

                    for (Map<String, Object> field : fields) {
                        Object fieldType = field.get("type");
                        bufferValueByType(fieldType, encoder);
                    }
                }
                case ARRAY -> {
                    Object itemType = fieldTypeMap.get("items");
                    for (long count = delegate.readArrayStart(); count != 0; count = delegate.arrayNext()) {
                        for (long i = 0; i < count; i++) {
                            bufferValueByType(itemType, encoder);
                        }
                    }
                }
                default -> throw new UnsupportedOperationException("Unsupported complex type: " + fieldTypeName);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        // Consume array item if we're in an array context
        if (!arrayContextStack.isEmpty()) {
            consumeArrayItem();
        }
    }

    @Override
    public void skipValue() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;
            consumeArrayItem();
            skipValue(itemType);
        } else {
            Object type = getFieldType(avroSchema, fieldIndex);
            skipValue(type);
        }
    }

    private void skipValue(Object type) throws IOException {
        if (type instanceof String fieldType) {
            skip(fieldType);
        } else if (type instanceof Map<?, ?> fieldTypeMap) {
            String fieldTypeName = (String) fieldTypeMap.get("type");
            switch (Type.fromString(fieldTypeName)) {
                case ARRAY -> {
                    for (long count = delegate.skipArray(); count != 0; count = delegate.arrayNext()) {
                        for (long i = 0; i < count; i++) {
                            Object itemType = fieldTypeMap.get("items");
                            skipValue(itemType);
                        }
                    }
                }
                case MAP -> {
                    for (long count = delegate.skipMap(); count != 0; count = delegate.mapNext()) {
                        delegate.readString(); // Skip the key
                        Object valuesType = fieldTypeMap.get("values");
                        skipValue(valuesType);
                    }
                }
                case RECORD -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> fields = (List<Map<String, Object>>) fieldTypeMap.get("fields");
                    for (Map<String, Object> field : fields) {
                        Object fieldType = field.get("type");
                        skipValue(fieldType);
                    }
                }
                default -> skip(fieldTypeName);
            }
        }
    }

    private void skip(String fieldTypeName) throws IOException {
        switch (Type.fromString(fieldTypeName)) {
            case NULL -> delegate.readNull();
            case BOOLEAN -> delegate.readBoolean();
            case INT -> delegate.readInt();
            case LONG -> delegate.readLong();
            case FLOAT -> delegate.readFloat();
            case DOUBLE -> delegate.readDouble();
            case STRING -> delegate.skipString();
            case ENUM -> delegate.readEnum();
            case BYTES -> delegate.skipBytes();
            default -> throw new UnsupportedOperationException("Unsupported type: " + fieldTypeName);
        }
    }

    @Override
    public void finishStructure(boolean consumeLeftElements) throws IOException {
        if (!arrayContextStack.isEmpty()) {
            if (consumeLeftElements) {
                ArrayContext context = arrayContextStack.peek();
                while (context.itemsRemaining > 0 || delegate.arrayNext() > 0) {
                    consumeArrayItem();
                }
            }
            arrayContextStack.pop();
        }
    }

    @Override
    public @NonNull IOException createDeserializationException(@NonNull String message, @Nullable Object invalidValue) {
        return new IOException(message + (invalidValue != null ? " (invalid value: " + invalidValue + ")" : ""));
    }

    private String handleString(Object itemType) throws IOException {
        if (itemType instanceof String type) {
            switch (Type.fromString(type)) {
                case STRING -> {
                    return delegate.readString();
                }
                case INT -> {
                    return String.valueOf(delegate.readInt());
                }
                case LONG -> {
                    return String.valueOf(delegate.readLong());
                }
                case FLOAT -> {
                    return String.valueOf(delegate.readFloat());
                }
                case DOUBLE -> {
                    return String.valueOf(delegate.readDouble());
                }
                case BOOLEAN -> {
                    return String.valueOf(delegate.readBoolean());
                }
                default -> throw new IllegalStateException("Unsupported type: " + itemType);
            }
        } else if (itemType instanceof Map<?, ?> map) {
            String fieldTypeName = (String) map.get("type");
                switch (Type.fromString(fieldTypeName)) {
                    case ARRAY, MAP -> throw new IllegalStateException("can't decode complex type to string  " + fieldTypeName);
                    case ENUM -> {
                        var symbols = map.get("symbols");
                        if (!(symbols instanceof List<?>)) {
                            throw new IllegalStateException("Symbols are missing or not a collection for ENUM type");

                        }
                        int index = delegate.readEnum();
                        return (String) ((List<?>) symbols).get(index);

                    }
                    default -> throw new IllegalStateException("Unsupported complex type: " + itemType);
                }
        }
        throw new IllegalStateException("Unsupported type: " + itemType);
    }

    private Object getFieldType(AvroSchema avroSchema, int fieldIndex) {
        Field field = avroSchema.getFields().get(fieldIndex);
        return field.getType();
    }

    private void consumeArrayItem() {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            context.itemsRemaining--;
        } else {
            throw new IllegalStateException("Not array element");
        }
    }

    private <T> T decodePrimitive(String expectedTypeName, Type avroType, AvroReader<T> reader) throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            consumeArrayItem();
            if (expectedTypeName.equals(context.itemType)) {
                return reader.read();
            }
            throw new IllegalStateException("Expected " + expectedTypeName + " array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (avroType == Type.fromString(fieldType)) {
                return reader.read();
            }
        } else if (type instanceof Map<?, ?> fieldType) {
            Object fieldMapType = fieldType.get("type");
            if (fieldMapType instanceof String stringFieldType) {
                if (avroType == Type.fromString(stringFieldType)) {
                    return reader.read();
                }
            }

        }
        throw new IllegalStateException("Expected " + expectedTypeName + " type but got: " + type);
    }

    @FunctionalInterface
    private interface AvroReader<T> {
        T read() throws IOException;
    }

    // Inner class to track array context state
    private static class ArrayContext {
        private long itemsRemaining;
        private final Object itemType; // Type of the items in this array

        public ArrayContext(long itemsRemaining, Object itemType) {
            this.itemsRemaining = itemsRemaining;
            this.itemType = itemType;
        }
    }

}
