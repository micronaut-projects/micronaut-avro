package io.micronaut.avro.serde;

import io.micronaut.avro.Avro;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.context.ApplicationContext;
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
            final Simple simple = mapper.readValue(result, Argument.of(Simple.class));
            assertNotNull(simple);
            assertEquals(
                "test",
                simple.value()
            );
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
            var deserializer = mapper.readValueFromTree(jsonNode, Simple.class);
            assertNotNull(jsonNode);
            assertTrue(jsonNode.isObject());
            assertEquals("test", jsonNode.get("value").getStringValue());
            assertEquals(object, deserializer);
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

            Salamander salamander = new Salamander("salamander", List.of(List.of("foo", "bar"), List.of("baz")), 23, List.of("str1", "str2"), 2.1f);

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
        List<List<String>> words,
        int age,
        List<String> strings,
        float salary
    ){}

    @Test
    public void testWriteValueAsBytesWithArgument() throws IOException {
        try (ApplicationContext ctx = ApplicationContext.run()) {
            AvroObjectMapper mapper = ctx.getBean(AvroObjectMapper.class);
            Salamander salamander = new Salamander("salamander", List.of(List.of("foo", "bar"), List.of("baz")), 23, List.of("str1", "str2"), 2.1f);

            byte[] bytes = mapper.writeValueAsBytes(Argument.of(Salamander.class), salamander);

            var deserializer = mapper.readValue(bytes, Salamander.class);
            assertEquals(salamander, deserializer);
        }
    }

}
