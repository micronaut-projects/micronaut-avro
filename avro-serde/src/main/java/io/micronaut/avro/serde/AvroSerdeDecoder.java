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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/***
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
            currentContext.itemsRemaining--;
            return true;
        }

        // Check if there are more blocks in the array
        long nextBlockCount = delegate.arrayNext();
        if (nextBlockCount > 0) {
            currentContext.itemsRemaining = nextBlockCount - 1; // -1 because we're returning true for the first item
            return true;
        }

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
            throw new IOException("Field index out of bounds or invalid: " + fieldIndex);
        }
        return avroSchema.getFields().get(fieldIndex).getName();
    }

    @Override
    public @NonNull String decodeString() throws IOException {
        // If we're in an array context, handle the item type appropriately
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            Object itemType = context.itemType;

            if (itemType instanceof String string) {
                return handleString(string);
            } else if (itemType instanceof Map<?, ?> map) {
                if ("string".equals(map.get("type"))) {
                    return delegate.readString();
                }
            }

            throw new IllegalStateException("Cannot decode array item to String: " + itemType);
        }

        // Regular field decoding
        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            return handleString(fieldType);
        }
        throw new IllegalStateException("Cannot decode complex Avro type '" + type + "' to String.");
    }

    @Override
    public boolean decodeBoolean() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("boolean".equals(context.itemType)) {
                return delegate.readBoolean();
            }
            throw new IllegalStateException("Expected boolean array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.BOOLEAN == Type.fromString(fieldType)) {
                return delegate.readBoolean();
            }
        }
        throw new IllegalStateException("Expected boolean type but got: " + type);
    }

    @Override
    public byte decodeByte() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("int".equals(context.itemType)) {
                return (byte) delegate.readInt();
            }
            throw new IllegalStateException("Expected int array item for byte but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.INT == Type.fromString(fieldType)) {
                return (byte) delegate.readInt();
            }
        }
        throw new IllegalStateException("Expected int type for byte but got: " + type);
    }

    @Override
    public short decodeShort() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("int".equals(context.itemType)) {
                return (short) delegate.readInt();
            }
            throw new IllegalStateException("Expected int array item for short but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.INT == Type.fromString(fieldType)) {
                return (short) delegate.readInt();
            }
        }
        throw new IllegalStateException("Expected int type for short but got: " + type);
    }

    @Override
    public char decodeChar() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("int".equals(context.itemType)) {
                return (char) delegate.readInt();
            }
            throw new IllegalStateException("Expected int array item for char but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.INT == Type.fromString(fieldType)) {
                return (char) delegate.readInt();
            }
        }
        throw new IllegalStateException("Expected int type for char but got: " + type);
    }

    @Override
    public int decodeInt() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("int".equals(context.itemType)) {
                return delegate.readInt();
            }
            throw new IllegalStateException("Expected int array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.INT == Type.fromString(fieldType)) {
                return delegate.readInt();
            }
        }
        throw new IllegalStateException("Expected int type but got: " + type);
    }

    @Override
    public long decodeLong() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("long".equals(context.itemType)) {
                return delegate.readLong();
            }
            throw new IllegalStateException("Expected long array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.LONG == Type.fromString(fieldType)) {
                return delegate.readLong();
            }
        }
        throw new IllegalStateException("Expected long type but got: " + type);
    }

    @Override
    public float decodeFloat() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("float".equals(context.itemType)) {
                return delegate.readFloat();
            }
            throw new IllegalStateException("Expected float array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.FLOAT == Type.fromString(fieldType)) {
                return delegate.readFloat();
            }
        }
        throw new IllegalStateException("Expected float type but got: " + type);
    }

    @Override
    public double decodeDouble() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            if ("double".equals(context.itemType)) {
                return delegate.readDouble();
            }
            throw new IllegalStateException("Expected double array item but got: " + context.itemType);
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof String fieldType) {
            if (Type.DOUBLE == Type.fromString(fieldType)) {
                return delegate.readDouble();
            }
        }
        throw new IllegalStateException("Expected double type but got: " + type);
    }

    @Override
    public @NonNull BigInteger decodeBigInteger() throws IOException {
        throw new UnsupportedOperationException("BigInteger decoding not implemented yet");
    }

    @Override
    public @NonNull BigDecimal decodeBigDecimal() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            throw new UnsupportedOperationException("BigDecimal array items not supported");
        }

        Object type = getFieldType(avroSchema, fieldIndex);
        if (type instanceof Map<?, ?> fieldType) {
            if (Type.STRING == Type.fromString(fieldType.get("type").toString()) &&
                LogicalType.DECIMAL == LogicalType.fromString(fieldType.get("logicalType").toString())) {
                String value = delegate.readString();
                return new BigDecimal(value);
            }
        }
        throw new IllegalStateException("Expected decimal type but got: " + type);
    }

    @Override
    public boolean decodeNull() throws IOException {
        if (!arrayContextStack.isEmpty()) {
            ArrayContext context = arrayContextStack.peek();
            return "null".equals(context.itemType);
        }

        AvroSchema.Field field = avroSchema.getFields().get(fieldIndex);
        return Type.NULL == field.getType();
    }

    @Override
    public @Nullable Object decodeArbitrary() throws IOException {
        return null;
    }

    @Override
    public @NonNull JsonNode decodeNode() throws IOException {
        return null;
    }

    @Override
    public Decoder decodeBuffer() throws IOException {
        return null;
    }

    @Override
    public void skipValue() throws IOException {

    }

    @Override
    public void finishStructure(boolean consumeLeftElements) throws IOException {
        if (!arrayContextStack.isEmpty()) {
            arrayContextStack.pop();
        }
    }

    @Override
    public @NonNull IOException createDeserializationException(@NonNull String message, @Nullable Object invalidValue) {
        return new IOException(message + (invalidValue != null ? " (invalid value: " + invalidValue + ")" : ""));
    }

    private String handleString(String itemType) throws IOException {
        switch (Type.fromString(itemType)) {
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
        }
        throw new IllegalStateException("Unsupported item type: " + itemType);

    }

    private Object getFieldType(AvroSchema avroSchema, int fieldIndex) {
        Field field = avroSchema.getFields().get(fieldIndex);
        return field.getType();
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

