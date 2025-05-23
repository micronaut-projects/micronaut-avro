package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;

@AvroSchemaSource("MyRecord-schema.avsc")
public record MyRecord(
    int testInt,
    float testFloat,
    double testDouble,
    long testLong,
    byte testByte,
    short testShort,
    char testChar
) {
}
