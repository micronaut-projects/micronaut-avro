package io.micronaut.avro.serde;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.core.type.Argument;
import io.micronaut.json.JsonStreamConfig;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.ObjectMapper;
import io.micronaut.serde.SerdeRegistry;
import io.micronaut.serde.Serializer;
import jakarta.inject.Singleton;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Singleton
public class AvroObjectMapper implements ObjectMapper {
    // https://github.com/micronaut-projects/micronaut-serialization/blob/2.14.x/serde-jsonp/src/main/java/io/micronaut/serde/json/stream/JsonStreamMapper.java

    private final SerdeRegistry registry;
    private final ResourceLoader resourceLoader;

    public AvroObjectMapper(SerdeRegistry registry, ResourceLoader resourceLoader) {
        this.registry = registry;
        this.resourceLoader = resourceLoader;
    }

    @Override
    public <T> T readValue(@NonNull InputStream inputStream, @NonNull Argument<T> type) throws IOException {
        Deserializer.DecoderContext decoderContext = registry.newDecoderContext(null);
        Deserializer<? extends T> deserializer = decoderContext.findDeserializer(type).createSpecific(decoderContext, type);
        return deserializer.deserialize(new AvroSerdeDecoder(DecoderFactory.get().binaryDecoder(inputStream, null), resourceLoader), decoderContext, type);
    }

    @Override
    public <T> void writeValue(@NonNull OutputStream outputStream, @NonNull Argument<T> type, @Nullable T object) throws IOException {
        Serializer.EncoderContext encoderContext = registry.newEncoderContext(null);
        Serializer<? super T> serializer = encoderContext.findSerializer(type).createSpecific(encoderContext, type);
        serializer.serialize(new AvroSerdeEncoder(EncoderFactory.get().binaryEncoder(outputStream, null), resourceLoader), encoderContext, type, object);
    }

    @Override
    public <T> T readValueFromTree(@NonNull JsonNode tree, @NonNull Argument<T> type) throws IOException {
        return null;
    }

    @Override
    public <T> T readValue(byte @NonNull [] byteArray, @NonNull Argument<T> type) throws IOException {
        return null;
    }

    @Override
    public @NonNull JsonNode writeValueToTree(@Nullable Object value) throws IOException {
        return null;
    }

    @Override
    public @NonNull <T> JsonNode writeValueToTree(@NonNull Argument<T> type, @Nullable T value) throws IOException {
        return null;
    }

    @Override
    public void writeValue(@NonNull OutputStream outputStream, @Nullable Object object) throws IOException {

    }

    @Override
    public byte[] writeValueAsBytes(@Nullable Object object) throws IOException {
        return new byte[0];
    }

    @Override
    public <T> byte[] writeValueAsBytes(@NonNull Argument<T> type, @Nullable T object) throws IOException {
        return new byte[0];
    }

    @Override
    public @NonNull JsonStreamConfig getStreamConfig() {
        return null;
    }
}
