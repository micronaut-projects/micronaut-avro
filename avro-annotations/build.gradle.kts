plugins {
    id("io.micronaut.build.internal.avro-module")
}

dependencies {
    compileOnly(mnSerde.micronaut.serde.jackson)
}
