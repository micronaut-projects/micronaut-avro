plugins {
    id("io.micronaut.build.internal.avro-module")
}

dependencies {
    api(mnSerde.micronaut.serde.api)
    api(mnSerde.micronaut.serde.support)
    api(libs.avro)
    implementation(projects.micronautAvroAnnotations)
    implementation(projects.micronautAvroSchemaCommon)
    testAnnotationProcessor(mn.micronaut.inject.java)
    testAnnotationProcessor(mnSerde.micronaut.serde.processor)
    testAnnotationProcessor(projects.micronautAvroSchemaProcessor)
}
