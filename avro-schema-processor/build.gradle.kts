plugins {
    id("io.micronaut.build.internal.avro-module")
}

dependencies {
    implementation(mn.micronaut.core.processor)
    implementation(mnSerde.micronaut.serde.jackson)
    implementation(projects.micronautAvroSchemaCommon)
    implementation(projects.micronautAvroAnnotations)

    testImplementation(mn.micronaut.inject.java.test)
    testImplementation(mnLogging.logback.classic)

}
