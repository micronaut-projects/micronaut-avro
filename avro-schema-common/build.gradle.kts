plugins {
    id("io.micronaut.build.internal.avro-module")
}
dependencies {
    annotationProcessor(mnSerde.micronaut.serde.processor)
    implementation(mnSerde.micronaut.serde.jackson)
}
