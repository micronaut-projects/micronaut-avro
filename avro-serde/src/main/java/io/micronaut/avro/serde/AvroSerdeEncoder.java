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

import io.micronaut.core.io.ResourceLoader;
import io.micronaut.serde.ObjectMapper;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.type.Argument;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.avro.model.AvroSchema;
import io.micronaut.serde.Encoder;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.TreeMap;

/***
 * Avro Implementation of {@link Encoder}
 * This class provides methods for encoding data into Avro format.
 *
 * @author Ali Linaboui
 * @since 1.0
 */

public final class AvroSerdeEncoder implements Encoder {

    private final org.apache.avro.io.Encoder delegate;
    private final boolean isArray;
    private final AvroSchema schema;
    private final AvroSerdeEncoder parent;
    private final ResourceLoader resourceLoader;

    private final Map<String, EncodingRunnable> objectBuffer = new TreeMap<>();
    private final List<EncodingRunnable> arrayBuffer = new ArrayList<>();
    private String currentKey;

    private AvroSerdeEncoder(org.apache.avro.io.Encoder delegate, boolean isArray, AvroSchema schema, AvroSerdeEncoder parent, ResourceLoader resourceLoader) {
        this.delegate = delegate;
        this.isArray = isArray;
        this.schema = schema;
        this.parent = parent;
        this.resourceLoader = resourceLoader;
    }

    /**
     * Constructs a new AvroSerdeEncoder instance.
     *
     * @param delegate Delegate encoder used for actual encoding.
     * @param resourceLoader resource loader used to load schema based on prefixes
     */
    public AvroSerdeEncoder(org.apache.avro.io.Encoder delegate, ResourceLoader resourceLoader) {
        this(delegate, false, null, null, resourceLoader);
    }

    @Override
    public @NonNull Encoder encodeArray(@NonNull Argument<?> type) throws IOException {
        AvroSerdeEncoder child = new AvroSerdeEncoder(delegate, true, null, this, resourceLoader);

        EncodingRunnable arrayWriter = () -> {
            delegate.writeArrayStart();
            delegate.setItemCount(child.arrayBuffer.size());
            for (EncodingRunnable r : child.arrayBuffer) {
                delegate.startItem();
                r.run();
            }
            delegate.writeArrayEnd();
        };

        if (isArray) {
            // Add the child array as an item
            arrayBuffer.add(arrayWriter);
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, arrayWriter);
            currentKey = null;
        }

        return child;
    }

    @Override
    public @NonNull Encoder encodeObject(@NonNull Argument<?> type) throws IOException {
        Class<?> targetClass = type.getType();
        if (targetClass.isAnnotationPresent(AvroSchemaSource.class)) {
            String schemaLocation = targetClass.getAnnotation(AvroSchemaSource.class).value();
            try (InputStream stream = resourceLoader.getResourceAsStream(schemaLocation).orElseThrow(() ->
                new IOException("Schema location " + schemaLocation + " not found"))) {
                AvroSchema avroSchema = ObjectMapper.getDefault().readValue(stream, AvroSchema.class);
                return new AvroSerdeEncoder(delegate, isArray, avroSchema, this, resourceLoader);
            }
        }

        return new AvroSerdeEncoder(delegate, false, null, this, resourceLoader);
    }

    @Override
    public void finishStructure() throws IOException {
        if (schema != null) {
            for (AvroSchema.Field field : schema.getFields()) {
                EncodingRunnable fieldEncoder = objectBuffer.get(field.getName());
                if (fieldEncoder == null) {
                    throw new IOException("Missing field: " + field.getName());
                }
                fieldEncoder.run();
            }
        } else {
            for (Map.Entry<String, EncodingRunnable> entry : objectBuffer.entrySet()) {
                entry.getValue().run();
            }
        }

        if (parent != null && parent.currentKey != null) {
            parent.objectBuffer.put(parent.currentKey, () -> { });
            parent.currentKey = null;
        } else if (parent == null) {
            delegate.flush();
        }

    }

    @Override
    public void encodeKey(@NonNull String key) throws IOException {
        this.currentKey = key;
    }

    @Override
    public void encodeString(@NonNull String value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> delegate.writeString(value));
    }

    @Override
    public void encodeBoolean(boolean value) throws IOException {
        validateType(value, AvroSchema.Type.BOOLEAN);
        buffer(() -> delegate.writeBoolean(value));
    }

    @Override
    public void encodeByte(byte value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> delegate.writeInt(value));
    }

    @Override
    public void encodeShort(short value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> delegate.writeInt(value));
    }

    @Override
    public void encodeChar(char value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> delegate.writeInt(value));
    }

    @Override
    public void encodeInt(int value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> delegate.writeInt(value));
    }

    @Override
    public void encodeLong(long value) throws IOException {
        validateType(value, AvroSchema.Type.LONG);
        buffer(() -> delegate.writeLong(value));
    }

    @Override
    public void encodeFloat(float value) throws IOException {
        validateType(value, AvroSchema.Type.FLOAT);
        buffer(() -> delegate.writeFloat(value));
    }

    @Override
    public void encodeDouble(double value) throws IOException {
        validateType(value, AvroSchema.Type.DOUBLE);
        buffer(() -> delegate.writeDouble(value));
    }

    @Override
    public void encodeBigInteger(@NonNull BigInteger value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> delegate.writeBytes(value.toByteArray()));
    }

    @Override
    public void encodeBigDecimal(@NonNull BigDecimal value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> delegate.writeString(value.toPlainString()));
    }

    @Override
    public void encodeNull() throws IOException {
        buffer(delegate::writeNull);
    }

    /**
     * Buffers the given value writer.
     *
     * @param valueWriter Value writer to buffer.
     */
    private void buffer(EncodingRunnable valueWriter) throws IOException {
        if (isArray) {
            arrayBuffer.add(() -> {
                delegate.startItem();
                valueWriter.run();
            });
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, valueWriter);
            currentKey = null;
        } else {
            valueWriter.run();
        }
    }

    /**
     * Validates the given value against the expected Avro type.
     *
     * @param value        Value to validate.
     * @param expectedType Expected Avro type.
     * @throws IOException If the value does not match the expected type.
     */
    private void validateType(Object value, AvroSchema.Type expectedType) throws IOException {
        if (value == null) {
            return; // null is allowed — Avro handles union with null
        }

        boolean isValid = isValidType(value, expectedType);

        if (!isValid) {
            throw new IOException("Invalid value type. Expected: " + expectedType + ", but got: " + value.getClass().getSimpleName());
        }
    }

    private boolean isValidType(Object value, AvroSchema.Type expectedType) throws IOException {
        if (value == null) {
            return expectedType == AvroSchema.Type.NULL;
        }

        Class<?> valueClass = value.getClass();
        return switch (expectedType) {
            case STRING ->
                isAssignable(valueClass, String.class) || isAssignable(valueClass, BigDecimal.class);
            case BOOLEAN -> isAssignable(valueClass, Boolean.class);
            case INT ->
                isAssignable(valueClass, Integer.class) || isAssignable(valueClass, Short.class)
                    || isAssignable(valueClass, Byte.class) || isAssignable(valueClass, Character.class)
                    || isAssignable(valueClass, BigInteger.class);
            case LONG -> isAssignable(valueClass, Long.class);
            case FLOAT -> isAssignable(valueClass, Float.class);
            case DOUBLE -> isAssignable(valueClass, Double.class);
            case BYTES, FIXED ->
                isAssignable(valueClass, byte[].class) || isAssignable(valueClass, Byte[].class);
            case ARRAY -> isAssignable(valueClass, Collection.class);
            case MAP ->
                isAssignable(valueClass, Map.class);
            case ENUM -> valueClass.isEnum();
            case NULL ->
                false;
            default -> throw new IOException("Unknown Avro type: " + expectedType);
        };
    }

    private boolean isAssignable(Class<?> clazz, Class<?> target) {
        return target.isAssignableFrom(clazz);
    }

}
