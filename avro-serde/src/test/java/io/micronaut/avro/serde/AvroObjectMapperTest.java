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
import java.util.List;

class AvroObjectMapperTest {
    @Test
    public void test() throws IOException {
        MyBean object = new MyBean("foo");

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
    record MyBean(String foo) {

    }
}
