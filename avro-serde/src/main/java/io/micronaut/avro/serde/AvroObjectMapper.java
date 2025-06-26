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

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.core.type.Argument;
import io.micronaut.json.JsonStreamConfig;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.SerdeRegistry;
import io.micronaut.serde.ObjectMapper;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.LimitingStream;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import io.micronaut.serde.config.SerdeConfiguration;
import io.micronaut.serde.support.util.JsonNodeDecoder;
import io.micronaut.serde.support.util.JsonNodeEncoder;
import jakarta.inject.Singleton;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

/**
 * The AvroObjectMapper class provides a concrete implementation of the {@link ObjectMapper} interface,
 * utilizing Avro serialization and deserialization.
 *
 * @author Ali Linaboui
 * @since 1.0
 */
@Singleton
public class AvroObjectMapper implements ObjectMapper {
    private final SerdeRegistry registry;
    private final ResourceLoader resourceLoader;
    @Nullable
    private final SerdeConfiguration serdeConfiguration;

    public AvroObjectMapper(SerdeRegistry registry, ResourceLoader resourceLoader) {
        this(registry, resourceLoader, null);

    }

    private AvroObjectMapper(SerdeRegistry registry, ResourceLoader resourceLoader, SerdeConfiguration serdeConfiguration) {
        this.registry = registry;
        this.resourceLoader = resourceLoader;
        this.serdeConfiguration = serdeConfiguration;
    }

    @Override
    public <T> T readValue(@NonNull InputStream inputStream, @NonNull Argument<T> type) throws IOException {
        Deserializer.DecoderContext decoderContext = registry.newDecoderContext(null);
        Deserializer<? extends T> deserializer = decoderContext.findDeserializer(type).createSpecific(decoderContext, type);
        try (AvroSerdeDecoder avroSerdeDecoder = new AvroSerdeDecoder(DecoderFactory.get().binaryDecoder(inputStream, null), resourceLoader)) {
            return deserializer.deserialize(avroSerdeDecoder, decoderContext, type);
        }
    }

    @Override
    public <T> void writeValue(@NonNull OutputStream outputStream, @NonNull Argument<T> type, @Nullable T object) throws IOException {
        Serializer.EncoderContext encoderContext = registry.newEncoderContext(null);
        Serializer<? super T> serializer = encoderContext.findSerializer(type).createSpecific(encoderContext, type);
        try (AvroSerdeEncoder avroSerdeEncoder = new AvroSerdeEncoder(EncoderFactory.get().binaryEncoder(outputStream, null), resourceLoader)) {
            serializer.serialize(avroSerdeEncoder, encoderContext, type, object);
        }
    }

    @Override
    public <T> T readValueFromTree(@NonNull JsonNode tree, @NonNull Argument<T> type) throws IOException {
        Deserializer.DecoderContext context = registry.newDecoderContext(null);
        final Deserializer<? extends T> deserializer = context.findDeserializer(type).createSpecific(context, type);
        return deserializer.deserialize(
            JsonNodeDecoder.create(tree, limits()),
            context,
            type
        );
    }

    @Override
    public <T> T readValue(byte @NonNull [] byteArray, @NonNull Argument<T> type) throws IOException {
        Deserializer.DecoderContext decoderContext = registry.newDecoderContext(null);
        Deserializer<? extends T> deserializer = decoderContext.findDeserializer(type).createSpecific(decoderContext, type);
        try (AvroSerdeDecoder avroSerdeDecoder = new AvroSerdeDecoder(DecoderFactory.get().binaryDecoder(byteArray, null), resourceLoader)) {
            return deserializer.deserialize(avroSerdeDecoder, decoderContext, type);
        }
    }

    @Override
    public @NonNull JsonNode writeValueToTree(@Nullable Object value) throws IOException {
        JsonNodeEncoder encoder = JsonNodeEncoder.create(limits());
        serialize(encoder, value);
        return encoder.getCompletedValue();
    }

    @Override
    public @NonNull <T> JsonNode writeValueToTree(@NonNull Argument<T> type, @Nullable T value) throws IOException {
        JsonNodeEncoder encoder = JsonNodeEncoder.create(limits());
        serialize(encoder, value, type);
        return encoder.getCompletedValue();

    }

    @Override
    public void writeValue(@NonNull OutputStream outputStream, @Nullable Object object) throws IOException {
        try (AvroSerdeEncoder avroSerdeEncoder = new AvroSerdeEncoder(EncoderFactory.get().binaryEncoder(outputStream, null), resourceLoader)) {
            serialize(avroSerdeEncoder, object);
        }
    }

    private void serialize(Encoder encoder, Object object) throws IOException {
        serialize(encoder, object, Argument.of(object.getClass()));
    }

    @SuppressWarnings("unchecked")
    private void serialize(Encoder encoder, Object object, Argument type) throws IOException {
        Serializer.EncoderContext context = registry.newEncoderContext(null);
        final Serializer<Object> serializer = context.findSerializer(type).createSpecific(context, type);
        serializer.serialize(
            encoder,
            context,
            type, object
        );
    }

    @Override
    public byte[] writeValueAsBytes(@Nullable Object object) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeValue(outputStream, object);
        return outputStream.toByteArray();
    }

    @Override
    public <T> byte[] writeValueAsBytes(@NonNull Argument<T> type, @Nullable T object) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeValue(outputStream, type, object);
        return outputStream.toByteArray();
    }

    @Override
    public @NonNull JsonStreamConfig getStreamConfig() {
        return JsonStreamConfig.DEFAULT;
    }

    @NonNull
    private LimitingStream.RemainingLimits limits() {
        return serdeConfiguration == null ? LimitingStream.DEFAULT_LIMITS : LimitingStream.limitsFromConfiguration(serdeConfiguration);
    }
}
