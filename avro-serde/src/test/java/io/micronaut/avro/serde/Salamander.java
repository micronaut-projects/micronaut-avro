package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;

import java.util.List;

@AvroSchemaSource("classpath:Test-schema.avsc")
public record Salamander(
    int age,
    String name,
    List<String> strings
) {
}
