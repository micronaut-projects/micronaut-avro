package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;

import java.util.List;

@AvroSchemaSource("classpath:NestedArray-schema.avsc")
public record NestedArrayModel(
    String name,
    List<List<String>> stringLists
) {
}
