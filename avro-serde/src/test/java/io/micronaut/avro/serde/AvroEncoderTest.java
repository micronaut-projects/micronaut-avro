package io.micronaut.avro.serde;
import io.micronaut.core.type.Argument;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.serde.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroEncoderTest {

    @Test
    void testEncodeStringValidInput() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try ( AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder) ) {
            avroEncoder.encodeString("foo");
        }

        byte[] encodedBytes = outputStream.toByteArray();
        assertEquals("[6, 102, 111, 111]", Arrays.toString(encodedBytes));
        System.out.println(Arrays.toString(encodedBytes));
    }

    @Test
    void testEncodeStringEmptyString() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try ( AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder) ) {
            avroEncoder.encodeString("");
        }

        byte[] encodedBytes = outputStream.toByteArray();
        System.out.println(Arrays.toString(encodedBytes));
        assertEquals("[0]", Arrays.toString(encodedBytes));
    }

    @Test
    void testEncodeIntValidInput() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try ( AvroSerdeEncoder avroEncoder = new AvroSerdeEncoder(encoder) ){
            avroEncoder.encodeInt(10);
        }

        byte[] encodedBytes = outputStream.toByteArray();
        assertEquals("[20]", Arrays.toString(encodedBytes));
    }

    @Test
    void testSalamanderEncoder() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            encoder.encodeKey("name");
            encoder.encodeString("foo");
            encoder.encodeKey("age");
            encoder.encodeInt(12);
            encoder.encodeKey("count");
            encoder.encodeFloat(23);
            encoder.encodeKey("by");
            encoder.encodeBoolean(true);
            encoder.encodeKey("bigDecimal");
            encoder.encodeBigDecimal(new BigDecimal("123.456"));

        }

        byte[] encodedBytes = outputStream.toByteArray();
        assertEquals("[24, 14, 49, 50, 51, 46, 52, 53, 54, 1, 0, 0, -72, 65, 6, 102, 111, 111]", Arrays.toString(encodedBytes));

    }

    @Test
    public void serializeArray() throws IOException {

          class Point {
            private final int x, y;

            private Point(int x, int y) {
                this.x = x;
                this.y = y;
            }
          }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // try-with-resource
        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            Argument<? extends Point> type = Argument.of(Point.class);
            Point value = new Point(3, 27);
            int[] coords = {2, 3, 5, 1, 5, 9};
            try (Encoder array = encoder.encodeArray(type)) {
                array.encodeInt(coords[0]);
                array.encodeInt(coords[1]);
                array.encodeInt(coords[2]);
                array.encodeInt(coords[3]);
                array.encodeInt(coords[4]);
                array.encodeInt(coords[5]);

            }
        }
        // Inspect raw bytes
        byte[] actualResult = outputStream.toByteArray();
            Assertions.assertNotNull(actualResult);
            Assertions.assertTrue(actualResult.length > 0);
        String expectedBytes = "[12, 4, 6, 10, 2, 10, 18, 0]";

        assertEquals(expectedBytes, Arrays.toString(actualResult));
    }

    @Test
    public void serializeObject() throws IOException {

        class Male {

            private String name;
            private int lag;

            public Male(String name, int age) {
                this.name = name;
                this.lag = age;
            }

            public String getName() {
                return name;
            }

            public int getAge() {
                return lag;
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);

        // try-with-resource
        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            Argument<? extends Male> type = Argument.of(Male.class);
            Male male = new Male("foo", 23);

            try (Encoder array = encoder.encodeObject(type)) {
                array.encodeKey("name");
                array.encodeString(male.getName());
                array.encodeKey("age");
                array.encodeInt(male.getAge());
            }
        }

        // Inspect raw bytes
        byte[] avroData = out.toByteArray();
        Assertions.assertNotNull(avroData);
        Assertions.assertTrue(avroData.length > 0);
        String actualResult = Arrays.toString(avroData);

        assertEquals("[46, 6, 102, 111, 111]", actualResult);
    }

    @Test
    public void serializeSalamanderClass() throws IOException {

        // Define the Salamander class with Avro schema annotation
        @AvroSchemaSource("classpath:Salamander-schema.avsc")
        class Salamander {
            private String name;
            private List<List<String>> words;
            private int age;
            private List<String> strings;
            private float salary;

            public Salamander(String name, List<List<String>> words, int age, List<String> stringSet, float salary) {
                this.name = name;
                this.words = words;
                this.age = age;
                this.strings = stringSet;
                this.salary = salary;
            }
        }

        // Initialize an instance of Salamander
        Salamander salamanderInstance = new Salamander("ali", List.of(List.of("now", "how", "law")), 23, List.of("bar", "foo"), 2100f);

        // Output stream for the serialized data
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            Argument<Salamander> type = Argument.of(Salamander.class);
            Encoder objectEncoder = encoder.encodeObject(type);

            // Encode properties
            objectEncoder.encodeKey("name");
            objectEncoder.encodeString(salamanderInstance.name);

            objectEncoder.encodeKey("words");
            Encoder outerArrayEncoder = objectEncoder.encodeArray(Argument.of(List.class));
            for (List<String> innerList : salamanderInstance.words) {

                Encoder innerArrayEncoder = outerArrayEncoder.encodeArray(Argument.of(String.class));
                for (String word : innerList) {
                    innerArrayEncoder.encodeString(word);
                }
                innerArrayEncoder.finishStructure();
            }
            outerArrayEncoder.finishStructure();
            objectEncoder.encodeKey("age");
            objectEncoder.encodeInt(salamanderInstance.age);

            objectEncoder.encodeKey("strings");
            Encoder setEncoder = objectEncoder.encodeArray(Argument.of(String.class));
            for (String item : salamanderInstance.strings) {
                setEncoder.encodeString(item);
            }

            objectEncoder.encodeKey("salary");
            objectEncoder.encodeFloat(salamanderInstance.salary);

            objectEncoder.finishStructure();
        }

        // Validate the result
        byte[] encodedBytes = out.toByteArray();
        String actualResult = Arrays.toString(encodedBytes);
        String expectedResult = "[6, 97, 108, 105, 2, 6, 6, 110, 111, 119, 6, 104, 111, 119, 6, 108, 97, 119, 0, 0, 46, 4, 6, 98, 97, 114, 6, 102, 111, 111, 0, 0, 64, 3, 69]";  // Replace with actual expected byte values
        assertEquals(expectedResult, actualResult);
    }
    @Test
    public void serializeBigDecimal() throws IOException {
        BigDecimal bd = new BigDecimal("12.1232");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        try(AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder)){
            encoder.encodeKey("bigDecimal");
            encoder.encodeBigDecimal(bd);
            encoder.encodeKey("float");
            encoder.encodeFloat(12.1f);
            encoder.finishStructure();
            byte[] avroData = outputStream.toByteArray();
            System.out.println("Serialized data: " + Arrays.toString(avroData));
        }
    }

    @Test
    public void serializeArrays() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            String[] strings = {"foo", "bar", "baz"};
            Argument<? extends String> argument = Argument.of(String.class);
            Encoder arrayEncoder = encoder.encodeArray(argument);
            arrayEncoder.encodeString(strings[0]);
            arrayEncoder.encodeString(strings[1]);
            arrayEncoder.encodeString(strings[2]);
        }

        byte[] avroData = outputStream.toByteArray();
        String actualResult = Arrays.toString(avroData);
        String expectedResult = "[6, 6, 102, 111, 111, 6, 98, 97, 114, 6, 98, 97, 122, 0]";
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void serializeNestedStructure() throws IOException {
        // Define the Salamander class with Avro schema annotation
        @AvroSchemaSource("classpath:Salamander-schema.avsc")
        class Salamander {
            private String name;
            private List<List<String>> words;


            public Salamander(String name, List<List<String>> words) {
                this.name = name;
                this.words = words;

            }
        }

        // Initialize an instance of Salamander
        Salamander salamanderInstance = new Salamander("ali", List.of(List.of("now", "how", "law"), List.of("a", "b", "c")));

        // Output stream for the serialized data
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
        try ( AvroSerdeEncoder encoder = new AvroSerdeEncoder(binaryEncoder) ) {
            Argument<Salamander> type = Argument.of(Salamander.class);
            Encoder objectEncoder = encoder.encodeObject(type);

            // Encode properties
            objectEncoder.encodeKey("name");
            objectEncoder.encodeString(salamanderInstance.name);

            objectEncoder.encodeKey("words");
            Encoder outerArrayEncoder = objectEncoder.encodeArray(Argument.of(String.class));
            for (List<String> innerList : salamanderInstance.words) {

                Encoder innerArrayEncoder = outerArrayEncoder.encodeArray(Argument.of(String.class));
                for (String word : innerList) {
                    innerArrayEncoder.encodeString(word);
                }
                innerArrayEncoder.finishStructure();
            }
            outerArrayEncoder.finishStructure();

            objectEncoder.finishStructure();
        }
        // Validate the result
        byte[] encodedBytes = out.toByteArray();
        String actualResult = Arrays.toString(encodedBytes);
        assertEquals("[6, 97, 108, 105, 4, 6, 6, 110, 111, 119, 6, 104, 111, 119, 6, 108, 97, 119, 0, 6, 2, 97, 2, 98, 2, 99, 0, 0]", actualResult);
    }

}
