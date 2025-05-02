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

import io.micronaut.serde.ObjectMapper;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.type.Argument;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.avro.model.AvroSchema;
import io.micronaut.serde.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;

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

    private final Map<String, EncodingRunnable> objectBuffer = new TreeMap<>();
    private final List<EncodingRunnable> arrayBuffer = new ArrayList<>();
    private String currentKey;

    private AvroSerdeEncoder(org.apache.avro.io.Encoder delegate, boolean isArray, AvroSchema schema, AvroSerdeEncoder parent) {
        this.delegate = delegate;
        this.isArray = isArray;
        this.schema = schema;
        this.parent = parent;
    }

    /**
     * Constructs a new AvroSerdeEncoder instance.
     *
     * @param delegate Delegate encoder used for actual encoding.
     */
    public AvroSerdeEncoder(org.apache.avro.io.Encoder delegate) {
        this(delegate, false, null, null);
    }

    @Override
    public @NonNull Encoder encodeArray(@NonNull Argument<?> type) throws IOException {
        AvroSerdeEncoder child = new AvroSerdeEncoder(delegate, true, null, this);

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
        } else {
            objectBuffer.put("__TOP_LEVEL_ARRAY__", arrayWriter);
        }

        return child;
    }

    @Override
    public @NonNull Encoder encodeObject(@NonNull Argument<?> type) throws IOException {
        Class<?> targetClass = type.getType();
        if (targetClass.isAnnotationPresent(AvroSchemaSource.class)) {
            String schemaName = targetClass.getAnnotation(AvroSchemaSource.class).value();
            String schema = readResource(targetClass.getClassLoader(), schemaName);
            ObjectMapper objectMapper = ObjectMapper.getDefault();
            AvroSchema avroSchema = objectMapper.readValue(schema, AvroSchema.class);
            return new AvroSerdeEncoder(delegate, isArray, avroSchema, this);
        }

        return new AvroSerdeEncoder(delegate, false, null, this);
    }

    @Override
    public void finishStructure() throws IOException {
        if (isArray) {
            if (objectBuffer.containsKey("__TOP_LEVEL_ARRAY__")){
                EncodingRunnable fieldEncoder = objectBuffer.get("__TOP_LEVEL_ARRAY__");
                fieldEncoder.run();
            }
            return;
        }

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
        objectBuffer.clear();

        if (parent != null && parent.currentKey != null) {
            parent.objectBuffer.put(parent.currentKey, () -> {});
            parent.currentKey = null;
        }
        delegate.flush();
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
        buffer(() -> delegate.writeFixed(new byte[]{value}));
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
        validateType(value, AvroSchema.Type.LONG);
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

    private String readResource(ClassLoader classLoader, String resourcePath) throws IOException {
        Iterator<URL> specs = classLoader.getResources(resourcePath).asIterator();
        if (!specs.hasNext()) {
            throw new IllegalArgumentException("Could not find resource " + resourcePath);
        }
        URL spec = specs.next();
        BufferedReader reader = new BufferedReader(new InputStreamReader(spec.openStream()));
        StringBuilder result = new StringBuilder();
        String inputLine;
        while ((inputLine = reader.readLine()) != null) {
            result.append(inputLine).append("\n");
        }
        reader.close();
        return result.toString();
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
        // todo to be updated and remove instance of
        return switch (expectedType) {
            case STRING -> value instanceof String;
            case BOOLEAN -> value instanceof Boolean;
            case INT -> value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof Character;
            case LONG -> value instanceof Long;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double;
            case BYTES, FIXED -> value instanceof byte[] || value instanceof Byte[];
            case NULL -> false;
            case ARRAY -> value instanceof Collection;
            case RECORD -> value instanceof Map || value instanceof Record;
            case ENUM -> value instanceof Enum;
            default -> throw new IOException("Unknown Avro type: " + expectedType);
        };
    }

}
