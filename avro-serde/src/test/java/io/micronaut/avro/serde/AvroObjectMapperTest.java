package io.micronaut.avro.serde;

import io.micronaut.avro.Avro;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.serde.annotation.Serdeable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

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

            Assertions.assertEquals(object, deserialized);
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

            Assertions.assertEquals(object, deserialized);
        }
    }

    @Serdeable
    @AvroSchemaSource("classpath:META-INF/avro-schemas/AvroObjectMapperTest/ComplexType-schema.avsc")
    @Avro
    record ComplexType(
        List<String> stringList
    ) {

    }
}
