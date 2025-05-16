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
import io.micronaut.avro.model.AvroSchema.Type;
import io.micronaut.serde.Encoder;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

        EncodingRunnable arrayWriter = new TypeAwareEncodingRunnable(Type.ARRAY, () -> {
            delegate.writeArrayStart();
            delegate.setItemCount(child.arrayBuffer.size());
            for (EncodingRunnable r : child.arrayBuffer) {
                delegate.startItem();
                r.run();
            }
            delegate.writeArrayEnd();
        });

        if (isArray) {
            // Add the child array as an item
            arrayBuffer.add(new TypeAwareEncodingRunnable(Type.ARRAY, arrayWriter));
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, new TypeAwareEncodingRunnable(Type.ARRAY, arrayWriter));
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
                if (field.getType() instanceof String) {
                    validateType(field.getType(), fieldEncoder);
                    fieldEncoder.run();
                } else if (field.getType() instanceof Map nestedSchema) {
                    validateType(nestedSchema.get("type"), fieldEncoder);
                    fieldEncoder.run();
                }
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
        buffer(() -> delegate.writeString(value), Type.STRING);
    }

    @Override
    public void encodeBoolean(boolean value) throws IOException {
        buffer(() -> delegate.writeBoolean(value), Type.BOOLEAN);
    }

    @Override
    public void encodeByte(byte value) throws IOException {
        buffer(() -> delegate.writeInt(value), Type.INT);
    }

    @Override
    public void encodeShort(short value) throws IOException {
        buffer(() -> delegate.writeInt(value), Type.INT);
    }

    @Override
    public void encodeChar(char value) throws IOException {
        buffer(() -> delegate.writeInt(value), Type.INT);
    }

    @Override
    public void encodeInt(int value) throws IOException {
        buffer(() -> delegate.writeInt(value), Type.INT);
    }

    @Override
    public void encodeLong(long value) throws IOException {
        buffer(() -> delegate.writeLong(value), Type.LONG);
    }

    @Override
    public void encodeFloat(float value) throws IOException {
        buffer(() -> delegate.writeFloat(value), Type.FLOAT);
    }

    @Override
    public void encodeDouble(double value) throws IOException {
        buffer(() -> delegate.writeDouble(value), Type.DOUBLE);
    }

    @Override
    public void encodeBigInteger(@NonNull BigInteger value) throws IOException {
        buffer(() -> delegate.writeBytes(value.toByteArray()), Type.BYTES);
    }

    @Override
    public void encodeBigDecimal(@NonNull BigDecimal value) throws IOException {
        buffer(() -> delegate.writeString(value.toPlainString()), Type.STRING);
    }

    @Override
    public void encodeNull() throws IOException {
        buffer(delegate::writeNull, Type.NULL);
    }

    /**
     * Buffers the given value writer.
     *
     * @param valueWriter Value writer to buffer.
     */
    private void buffer(EncodingRunnable valueWriter, Type type) throws IOException {
        if (isArray) {
            arrayBuffer.add(new TypeAwareEncodingRunnable(type, () -> {
                delegate.startItem();
                valueWriter.run();
            }));
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, new TypeAwareEncodingRunnable(type, valueWriter));
            currentKey = null;
        } else {
            valueWriter.run();
        }
    }

    private boolean isValidType(Object schemaType, Type type) {
        try {
            return Type.fromString((String) schemaType) == type;
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Unsupported schema type: " + schemaType, e);
        }
    }

    private void validateType(Object schemaType, EncodingRunnable fieldEncoder) throws IOException {
        // Get the type of the field encoder
        Type type = getTypeFromEncoder(fieldEncoder);

        // Validate the type against the schema type
        if (!isValidType(schemaType, type)) {
            throw new IOException("Type mismatch for field. Expected " + schemaType + " but got " + type);
        }
    }

    private Type getTypeFromEncoder(EncodingRunnable fieldEncoder) {
        if (fieldEncoder instanceof TypeAwareEncodingRunnable) {
            return ((TypeAwareEncodingRunnable) fieldEncoder).getType();
        } else {
            throw new UnsupportedOperationException("Type inference not supported for this encoder");
        }
    }

}

class TypeAwareEncodingRunnable implements EncodingRunnable {
    private final Type type;
    private final EncodingRunnable runnable;

    public TypeAwareEncodingRunnable(Type type, EncodingRunnable runnable) {
        this.type = type;
        this.runnable = runnable;
    }

    @Override
    public void run() throws IOException {
        runnable.run();
    }

    public Type getType() {
        return type;
    }
}
