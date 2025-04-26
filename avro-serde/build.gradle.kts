plugins {
    id("io.micronaut.build.internal.avro-module")
}

dependencies {
    api(mnSerde.micronaut.serde.api)
    api(libs.avro)
    api(projects.micronautAvroAnnotations)
    api(projects.micronautAvroSchemaCommon)
}
