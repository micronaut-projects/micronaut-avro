plugins {
    id("io.micronaut.build.internal.avro-module")
}
dependencies {
    annotationProcessor(mnSerde.micronaut.serde.processor)
    annotationProcessor("io.micronaut.sourcegen:micronaut-sourcegen-generator-java")
    compileOnly("io.micronaut.sourcegen:micronaut-sourcegen-annotations")
    implementation(mnSerde.micronaut.serde.jackson)
}
