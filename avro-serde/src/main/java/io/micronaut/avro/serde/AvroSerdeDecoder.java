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

/***
 * Avro Implementation of {@link Decoder}
 * This class provides methods for decoding data into Avro format.
 *
 * @author Ali Linaboui
 * @since 1.0
 */
public class AvroSerdeDecoder implements Decoder {

    private final AvroSchema avroSchema;
    int fieldIndex = -1;
    private final org.apache.avro.io.Decoder delegate;
    private final ResourceLoader resourceLoader;

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
        return null;
    }

    @Override
    public boolean hasNextArrayValue() throws IOException {
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
        return new AvroSerdeDecoder(delegate, resourceLoader, null);
    }

    @Override
    public @Nullable String decodeKey() throws IOException {
        fieldIndex++;
        return avroSchema.getFields().get(fieldIndex).getName();
    }

    @Override
    public @NonNull String decodeString() throws IOException {
        AvroSchema.Field field = avroSchema.getFields().get(fieldIndex);
        switch (AvroSchema.Type.fromString((String) field.getType())) {
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
            case RECORD, ARRAY, MAP, ENUM -> {
                throw new IllegalStateException("");
            }
            default -> {
                throw new IllegalStateException("Unsupported Type: " + field.getType());
            }
        }
    }

    @Override
    public boolean decodeBoolean() throws IOException {
        return false;
    }

    @Override
    public byte decodeByte() throws IOException {
        return 0;
    }

    @Override
    public short decodeShort() throws IOException {
        return 0;
    }

    @Override
    public char decodeChar() throws IOException {
        return 0;
    }

    @Override
    public int decodeInt() throws IOException {
        return delegate.readInt();
    }

    @Override
    public long decodeLong() throws IOException {
        return 0;
    }

    @Override
    public float decodeFloat() throws IOException {
        return 0;
    }

    @Override
    public double decodeDouble() throws IOException {
        return 0;
    }

    @Override
    public @NonNull BigInteger decodeBigInteger() throws IOException {
        return null;
    }

    @Override
    public @NonNull BigDecimal decodeBigDecimal() throws IOException {
        return null;
    }

    @Override
    public boolean decodeNull() throws IOException {
        return false;
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

    }

    @Override
    public @NonNull IOException createDeserializationException(@NonNull String message, @Nullable Object invalidValue) {
        return null;
    }
}
