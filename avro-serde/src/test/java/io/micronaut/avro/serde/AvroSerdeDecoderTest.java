package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class AvroSerdeDecoderTest {

    @Test
    void decodeObjectAfterSerialization() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            try(Encoder objEncoder = avroEncoder.encodeObject(Argument.of(Person.class))) {
                objEncoder.encodeKey("name");
                objEncoder.encodeString("ali");
                objEncoder.encodeKey("age");
                objEncoder.encodeInt(23);
            }
        }

        byte[] encodedBytes = outputStream.toByteArray();
        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(Person.class))) {

                String keyAge = objDecoder.decodeKey();
                int age = objDecoder.decodeInt();
                String keyName = objDecoder.decodeKey();
                String name = objDecoder.decodeString();

                Assertions.assertEquals("name", keyName);
                Assertions.assertEquals("ali", name);
                Assertions.assertEquals("age", keyAge);
                Assertions.assertEquals(23, age);
            }
        }

    }
    @Test
    void decodeObjectWithArrayAfterSerialization() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        Salamander salamanderInstance = new Salamander(23, "ali", List.of("foo","bar"));

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            try(Encoder objEncoder = avroEncoder.encodeObject(Argument.of(Salamander.class))) {
                objEncoder.encodeKey("name");
                objEncoder.encodeString(salamanderInstance.name());
                objEncoder.encodeKey("age");
                objEncoder.encodeInt(salamanderInstance.age());
                objEncoder.encodeKey("strings");
                try(Encoder objArrayEncoder = objEncoder.encodeArray(Argument.of(String.class))) {
                    for (String string : salamanderInstance.strings()) {
                        objArrayEncoder.encodeString(string);
                    }
                }
            }
        }

        byte[] encodedBytes = outputStream.toByteArray();
        Assertions.assertEquals("[46, 6, 97, 108, 105, 4, 6, 102, 111, 111, 6, 98, 97, 114, 0]", Arrays.toString(encodedBytes));

        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(Salamander.class))) {

                String keyAge = objDecoder.decodeKey();
                int age = objDecoder.decodeInt();
                String keyName = objDecoder.decodeKey();
                String name = objDecoder.decodeString();
                String keyStrings = objDecoder.decodeKey();
                List<String> strings = new ArrayList<>();
                try(Decoder arrayDecoder = objDecoder.decodeArray(Argument.of(String.class))){
                    while (arrayDecoder.hasNextArrayValue()) {
                        strings.add(arrayDecoder.decodeString());
                    }
                }

                Assertions.assertEquals("age", keyAge);
                Assertions.assertEquals(23, age);
                Assertions.assertEquals("name", keyName);
                Assertions.assertEquals("ali", name);
                Assertions.assertEquals("strings", keyStrings);
                Assertions.assertArrayEquals(new List[]{strings}, new List[]{salamanderInstance.strings()});
            }
        }

    }
    @Test
    void decodeObjectWithNestedArrays() throws IOException {
        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // Create nested lists
        List<List<String>> nestedLists = new ArrayList<>();
        nestedLists.add(List.of("one", "two"));
        nestedLists.add(List.of("three", "four", "five"));

        NestedArrayModel model = new NestedArrayModel("test-model", nestedLists);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())) {
            try (Encoder objEncoder = avroEncoder.encodeObject(Argument.of(NestedArrayModel.class))) {
                objEncoder.encodeKey("name");
                objEncoder.encodeString(model.name());
                objEncoder.encodeKey("strings");

                // Encode outer array
                try (Encoder outerArrayEncoder = objEncoder.encodeArray(Argument.of(List.class))) {
                    for (List<String> innerList : model.stringLists()) {
                        // Encode each inner array
                        try (Encoder innerArrayEncoder = outerArrayEncoder.encodeArray(Argument.of(String.class))) {
                            for (String item : innerList) {
                                innerArrayEncoder.encodeString(item);
                            }
                        }
                    }
                }
            }
        }

        byte[] encodedBytes = outputStream.toByteArray();

        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(NestedArrayModel.class))) {
                String keyName = objDecoder.decodeKey();
                String name = objDecoder.decodeString();
                String keyStringLists = objDecoder.decodeKey();

                List<List<String>> decodedLists = new ArrayList<>();
                try (Decoder outerArrayDecoder = objDecoder.decodeArray(Argument.of(List.class))) {
                    while (outerArrayDecoder.hasNextArrayValue()) {
                        List<String> innerList = new ArrayList<>();
                        try (Decoder innerArrayDecoder = outerArrayDecoder.decodeArray(Argument.of(String.class))) {
                            while (innerArrayDecoder.hasNextArrayValue()) {
                                innerList.add(innerArrayDecoder.decodeString());
                            }
                        }
                        decodedLists.add(innerList);
                    }
                }

                Assertions.assertEquals("name", keyName);
                Assertions.assertEquals("test-model", name);
                Assertions.assertEquals("strings", keyStringLists);

                // Verify the nested lists
                Assertions.assertEquals(2, decodedLists.size());
                Assertions.assertEquals(List.of("one", "two"), decodedLists.get(0));
                Assertions.assertEquals(List.of("three", "four", "five"), decodedLists.get(1));
                Assertions.assertEquals(model.stringLists(), decodedLists);
            }
        }
    }

    @Test
    void decodeObjectOfPrimitivesAfterSerialization() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            try(Encoder objEncoder = avroEncoder.encodeObject(Argument.of(MyRecord.class))) {

                objEncoder.encodeKey("testInt");
                objEncoder.encodeInt(5);

                objEncoder.encodeKey("testFloat");
                objEncoder.encodeFloat(21f);

                objEncoder.encodeKey("testDouble");
                objEncoder.encodeDouble(5d);

                objEncoder.encodeKey("testLong");
                objEncoder.encodeLong(5L);

                objEncoder.encodeKey("testByte");
                objEncoder.encodeByte((byte) 1);

                objEncoder.encodeKey("testShort");
                objEncoder.encodeShort((short) 5);

                objEncoder.encodeKey("testChar");
                objEncoder.encodeChar('a');

            }
        }
        byte[] encodedBytes = outputStream.toByteArray();

        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(MyRecord.class))) {

                String keyByte = objDecoder.decodeKey();
                byte byteValue = objDecoder.decodeByte();

                String keyChar = objDecoder.decodeKey();
                char charValue = objDecoder.decodeChar();

                String keyDouble = objDecoder.decodeKey();
                double doubleValue = objDecoder.decodeDouble();

                String keyFloat = objDecoder.decodeKey();
                float floatValue = objDecoder.decodeFloat();

                String keyInt = objDecoder.decodeKey();
                int intValue = objDecoder.decodeInt();

                String keyLong = objDecoder.decodeKey();
                long longValue = objDecoder.decodeLong();

                String keyShort = objDecoder.decodeKey();
                short shortValue = objDecoder.decodeShort();

                Assertions.assertEquals("testByte", keyByte);
                Assertions.assertEquals((byte) 1, byteValue);

                Assertions.assertEquals("testChar", keyChar);
                Assertions.assertEquals('a', charValue);

                Assertions.assertEquals("testDouble", keyDouble);
                Assertions.assertEquals(5d, doubleValue);

                Assertions.assertEquals("testFloat", keyFloat);
                Assertions.assertEquals(21f, floatValue);

                Assertions.assertEquals("testInt", keyInt);
                Assertions.assertEquals(5, intValue);

                Assertions.assertEquals("testLong", keyLong);
                Assertions.assertEquals(5L, longValue);

                Assertions.assertEquals("testShort", keyShort);
                Assertions.assertEquals((short) 5, shortValue);

            }
        }

    }

    @Test
    void decodeBigIntegerAndBigDecimalAfterSerializationAndEnum() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        BigInteger bigInteger = new BigInteger("12345678901234567890");
        BigDecimal bigDecimal = new BigDecimal("12345678901234567890.1234567890");

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            try(Encoder objEncoder = avroEncoder.encodeObject(Argument.of(BigIntegerAndBigDecimalHolder.class))) {
                objEncoder.encodeKey("bigInt");
                objEncoder.encodeBigInteger(bigInteger);
                objEncoder.encodeKey("bigDecimal");
                objEncoder.encodeBigDecimal(bigDecimal);
                objEncoder.encodeKey("color");
                objEncoder.encodeString("BLUE");
            }
        }

        byte[] encodedBytes = outputStream.toByteArray();
        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(BigIntegerAndBigDecimalHolder.class))) {

                String keyBigInt = objDecoder.decodeKey();
                BigInteger decodedBigInteger = objDecoder.decodeBigInteger();
                String keyBigDecimal = objDecoder.decodeKey();
                BigDecimal decodedBigDecimal = objDecoder.decodeBigDecimal();
                String keyEnum = objDecoder.decodeKey();
                String decodedEnum = objDecoder.decodeString();

                Assertions.assertEquals("bigInt", keyBigInt);
                Assertions.assertEquals(bigInteger, decodedBigInteger);
                Assertions.assertEquals("bigDecimal", keyBigDecimal);
                Assertions.assertEquals(bigDecimal, decodedBigDecimal);
                Assertions.assertEquals("color", keyEnum);
                Assertions.assertEquals("BLUE", decodedEnum);
            }
        }

    }

    @Test
    void decodeObjectOfPrimitivesAfterSerializationWithSkip() throws IOException {

        /* Serialize data */
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder, ctx.getEnvironment())){
            try(Encoder objEncoder = avroEncoder.encodeObject(Argument.of(MyRecord.class))) {

                objEncoder.encodeKey("testInt");
                objEncoder.encodeInt(5);

                objEncoder.encodeKey("testFloat");
                objEncoder.encodeFloat(21f);

                objEncoder.encodeKey("testDouble");
                objEncoder.encodeDouble(5d);

                objEncoder.encodeKey("testLong");
                objEncoder.encodeLong(5L);

                objEncoder.encodeKey("testByte");
                objEncoder.encodeByte((byte) 1);

                objEncoder.encodeKey("testShort");
                objEncoder.encodeShort((short) 5);

                objEncoder.encodeKey("testChar");
                objEncoder.encodeChar('a');

            }
        }
        byte[] encodedBytes = outputStream.toByteArray();

        /* Deserialize data */
        ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);
        org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        try (ApplicationContext ctx = ApplicationContext.run();
             AvroSerdeDecoder avroDecoder = new AvroSerdeDecoder(decoder, ctx.getEnvironment())) {
            try (Decoder objDecoder = avroDecoder.decodeObject(Argument.of(MyRecord.class))) {

                objDecoder.decodeKey();
                objDecoder.skipValue(); // skip byte

                String keyChar = objDecoder.decodeKey();
                char charValue = objDecoder.decodeChar();

                String keyDouble = objDecoder.decodeKey();
                double doubleValue = objDecoder.decodeDouble();

                objDecoder.decodeKey();
                objDecoder.skipValue(); // skip float

                String keyInt = objDecoder.decodeKey();
                int intValue = objDecoder.decodeInt();

                objDecoder.decodeKey();
                objDecoder.skipValue(); //skip long

                objDecoder.decodeKey();
                objDecoder.skipValue(); // skip short

                Assertions.assertEquals("testInt", keyInt);
                Assertions.assertEquals(5, intValue);

                Assertions.assertEquals("testDouble", keyDouble);
                Assertions.assertEquals(5d, doubleValue);

                Assertions.assertEquals("testChar", keyChar);
                Assertions.assertEquals('a', charValue);

            }
        }

    }

    @AvroSchemaSource("big-integer-and-big-decimal-holder.avsc")
    static class BigIntegerAndBigDecimalHolder {
        private BigInteger bigInt;
        private BigDecimal bigDecimal;
        private Color color;

        public BigInteger getBigInt() {
            return bigInt;
        }

        public void setBigInt(BigInteger bigInt) {
            this.bigInt = bigInt;
        }

        public BigDecimal getBigDecimal() {
            return bigDecimal;
        }

        public void setBigDecimal(BigDecimal bigDecimal) {
            this.bigDecimal = bigDecimal;
        }
        public Color getColor() {
            return color;
        }

        public void setColor(Color color) {
            this.color = color;
        }

        enum Color {
            GREEN, BLUE, RED, YELLOW
        }
    }
}
