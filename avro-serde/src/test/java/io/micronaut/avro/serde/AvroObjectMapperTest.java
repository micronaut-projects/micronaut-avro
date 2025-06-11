package io.micronaut.avro.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.avro.Avro;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.annotation.Creator;
import io.micronaut.core.type.Argument;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.annotation.Serdeable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AvroObjectMapperTest {
    @Test
    public void test() throws IOException {
        BigInteger bigInteger = new BigInteger("12345678901234567890");
        BigDecimal bigDecimal = new BigDecimal("12345678901234567890.1234567890");
        MyBean object = new MyBean("ali", 23, 2.1f, true, 5d, 5L, (short) 5, (byte) 4, 'a', bigDecimal, bigInteger, MyBean.Color.BLUE);

        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            mapper.writeValue(stream, Argument.of(MyBean.class), object);
            var deserialized = mapper.readValue(new ByteArrayInputStream(stream.toByteArray()), Argument.of(MyBean.class));

            assertEquals(object, deserialized);
        }
    }

    @Serdeable
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/MyBean-schema.avsc")
    @Avro
    record MyBean(
        String foo,
        int age,
        float count,
        boolean bool,
        double d,
        long l,
        short s,
        byte b,
        char c,
        BigDecimal bigDecimal,
        BigInteger bigInteger,
        Color color
    ) {
        enum Color{
            RED,
            GREEN,
            BLUE
        }
    }
    @Test
    public void testComplexType() throws IOException {
        ComplexType object = new ComplexType(List.of("foo", "bar", "baz"));

        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            mapper.writeValue(stream, Argument.of(ComplexType.class), object);
            var deserialized = mapper.readValue(new ByteArrayInputStream(stream.toByteArray()), Argument.of(ComplexType.class));

            assertEquals(object, deserialized);
        }
    }

    @Serdeable
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/ComplexType-schema.avsc")
    @Avro
    record ComplexType(
        List<String> stringList
    ) {

    }
    @Test
    public void testNestedStructure() throws IOException {
        NestedType object = new NestedType(List.of(List.of(List.of("foo", "bar", "baz"), List.of("foo", "bar", "baz"))), MyBean.Color.GREEN);

        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            mapper.writeValue(stream, Argument.of(NestedType.class), object);
            var deserialized = mapper.readValue(new ByteArrayInputStream(stream.toByteArray()), Argument.of(NestedType.class));

            assertEquals(object, deserialized);
        }
    }

    @Serdeable
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/NestedType-schema.avsc")
    @Avro
    record NestedType(
        List<List<List<String>>> nestedList,
        MyBean.Color color
    ) {

    }

    @Test
    void testSimpleMapping() throws IOException {

        Simple s = new Simple("test");
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            final byte[] result = mapper.writeValueAsBytes(s);
            assertEquals("[8, 116, 101, 115, 116]", Arrays.toString(result));
            final Simple deserialized = mapper.readValue(result, Argument.of(Simple.class));
            assertNotNull(deserialized);
            assertEquals(s, deserialized);
        }

    }

    @Serdeable
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/Simple-schema.avsc")
    @Avro
    record Simple(String value) {}

    @Test
    void testWriteValueToTree() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            Simple object = new Simple("test");

            JsonNode jsonNode = mapper.writeValueToTree(object);
            var deserialized = mapper.readValueFromTree(jsonNode, Simple.class);
            assertNotNull(jsonNode);
            assertTrue(jsonNode.isObject());
            assertEquals("test", jsonNode.get("value").getStringValue());
            assertEquals(object, deserialized);
        }
    }

    @Test
    void testWriteValueToTreeWithArgument() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            Simple object = new Simple("test");

            JsonNode jsonNode = mapper.writeValueToTree(Argument.of(Simple.class), object);
            var deserialized = mapper.readValueFromTree(jsonNode, Argument.of(Simple.class));

            assertNotNull(jsonNode);
            assertTrue(jsonNode.isObject());
            assertEquals("test", jsonNode.get("value").getStringValue());
            assertEquals(object, deserialized);
        }
    }

    @Test
    public void testWriteValueWithOutputStreamAndObject() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            Salamander salamander = new Salamander("salamander", List.of(List.of(List.of("foo", "bar"), List.of("baz"))), 23, List.of("str1", "str2"), 2.1f);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            mapper.writeValue(stream, salamander);

            var deserialized = mapper.readValue(new ByteArrayInputStream(stream.toByteArray()), Salamander.class);
            assertEquals(salamander, deserialized);
        }
    }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/Salamander-schema.avsc")
    @Avro
    @Serdeable
    record Salamander (
        String name,
        List<List<List<String>>> words,
        int age,
        List<String> strings,
        float salary
    ){}

    @Test
    public void testWriteValueAsBytesWithArgument() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            Salamander salamander = new Salamander("salamander", List.of(List.of(List.of("foo", "bar"), List.of("baz"))), 23, List.of("str1", "str2"), 2.1f);

            byte[] bytes = mapper.writeValueAsBytes(Argument.of(Salamander.class), salamander);

            var deserialized = mapper.readValue(bytes, Salamander.class);
            assertEquals(salamander, deserialized);
        }
    }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/Salamanders-schema.avsc")
    record Salamanders (
        String name,
        List<List<List<String>>> words,
        int age,
        List<String> strings,
        @JsonIgnore
        float salary
    ){}

    @Test
    public void shouldNotSerializeSalaryFieldAndThrowIOExceptionMissingField() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            Salamanders salamander = new Salamanders("salamander", List.of(List.of(List.of("foo", "bar"), List.of("baz"))), 23, List.of("str1", "str2"), 2.1f);

            assertThrows(IOException.class,()-> mapper.writeValueAsBytes(Argument.of(Salamanders.class), salamander), "Missing field: salary");
        }
    }

    @Test
    void testSkipValue() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            MixedTypes original = new MixedTypes(
                "str1",
                5,
                true,
                "str2"
            );

            byte[] stream = mapper.writeValueAsBytes(original);
            SkipTypes deserialized = mapper.readValue(
                stream,
                Argument.of(SkipTypes.class)
            );

            assertEquals(original.str1(), deserialized.str1());
            assertEquals(original.str2(), deserialized.str2());
        }
    }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/MixedTypes-schema.avsc")
    @Avro
    record MixedTypes(
        String str1,
        int skipInt,
        boolean skipBool,
        String str2
    ) {}

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/MixedTypes-schema.avsc")
    @Serdeable
    record SkipTypes(
        String str1,
        String str2
    ) {
    }

    @Test
    void testArraySkipValue() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            MixedArrayTypes original = new MixedArrayTypes(
                2.1f,
                List.of(1, 2, 3),
                List.of(true, false, true),
                'c'
            );

            byte[] stream = mapper.writeValueAsBytes(original);
            MixedSkipArrayTypes deserialized = mapper.readValue(
                stream,
                Argument.of(MixedSkipArrayTypes.class)
            );
            System.out.println(deserialized);
            assertEquals(original.f, deserialized.f);
            assertNotEquals(original.listInteger, deserialized.skipInteger);
            assertNotEquals(original.booleanList, deserialized.skipBoolean);
            assertEquals(original.c, deserialized.c);

        }
    }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/MixedArrayTypes-schema.avsc")
    @Avro
    record MixedArrayTypes(
        float f,
        List<Integer> listInteger,
        List<Boolean> booleanList,
        char c
    ){ }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/MixedSkipArrayTypes-schema.avsc")
    @Avro
    record MixedSkipArrayTypes(
        float f,
        @JsonIgnore
        List<Integer> skipInteger,
        @JsonIgnore
        List<Boolean> skipBoolean,
        char c
    ){
        @Creator
        public static MixedSkipArrayTypes create(@JsonProperty("2.1f") float f ,@JsonProperty("c") char c) {
            return new MixedSkipArrayTypes(2.1f, null, null, 'c');
        }
    }

    @Test
    void testStringCaseInt() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            StringPrimitiveCases original = new StringPrimitiveCases(12, 2.1f, 5d, true, 5L);
            byte[] stream = mapper.writeValueAsBytes(original);

            StringCases deserialized = mapper.readValue(stream, Argument.of(StringCases.class));

            assertEquals(original.s, Integer.valueOf(deserialized.s));
            assertEquals(original.f, Float.valueOf(deserialized.f));
            assertEquals(original.d, Double.valueOf(deserialized.d));
            assertEquals(original.bool, Boolean.valueOf(deserialized.bool));
            assertEquals(original.l, Long.valueOf(deserialized.l));
        }
    }

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/StringPrimitiveCases-schema.avsc")
    @Serdeable
    @Avro
    record StringPrimitiveCases(
        int s,
        float f,
        double d,
        Boolean bool,
        long l
    ){}

    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/StringPrimitiveCases-schema.avsc")
    @Serdeable
    record StringCases(
        String s,
        String f,
        String d,
        String bool,
        String l
    ){}

    @Test
    void testArrayBigIntegerAndBigDecimal() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);

            BigInteger bigInteger1 = new BigInteger("12345678901234567891");
            BigInteger bigInteger2 = new BigInteger("12345678901233422221");
            BigInteger bigInteger3 = new BigInteger("12345678901234567893");

            BigDecimal bigDecimal1 = new BigDecimal("12345678901234567891.1234567891");
            BigDecimal bigDecimal2 = new BigDecimal("12345678901234567892.1234567892");
            BigDecimal bigDecimal3 = new BigDecimal("12345678901234567893.1234567893");

            ArrayBigIntegerTypes original = new ArrayBigIntegerTypes(
                List.of(bigInteger1, bigInteger2, bigInteger3),
                List.of(bigDecimal1, bigDecimal2, bigDecimal3),
                List.of(1, 2, 3)
            );

            byte[] stream = mapper.writeValueAsBytes(original);

            ArrayBigIntegerTypes deserialized = mapper.readValue(stream, Argument.of(ArrayBigIntegerTypes.class));

            assertEquals(original, deserialized);
        }
    }

    @Avro
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/ArrayBigIntegerTypes-schema.avsc")
    record ArrayBigIntegerTypes(
        List<BigInteger> arrBigDecimal,
        List<BigDecimal> arrBigInteger,
        List<Integer> list
    ){}
}
