package io.micronaut.avro.serde;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.serde.Decoder;
import io.micronaut.serde.Encoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class AvroSerdeDecoderTest {

    @Test
    void testEncodeStringValidInput() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            Encoder objEncoder = avroEncoder.encodeObject(Argument.of(Person.class));
            objEncoder.encodeKey("name");
            objEncoder.encodeString("ali");
            objEncoder.encodeKey("age");
            objEncoder.encodeInt(23);
            objEncoder.finishStructure();
        }

        byte[] encodedBytes = outputStream.toByteArray();
        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())){
            Decoder objDecoder = avroDecoder.decodeObject(Argument.of(Person.class));

            objDecoder.decodeKey();
            String age = objDecoder.decodeString();
            objDecoder.decodeKey();
            String name = objDecoder.decodeString();

            Assertions.assertEquals("ali", name);
            Assertions.assertEquals("23", age);
        }


    }
}
