plugins {
    id("io.micronaut.build.internal.avro-module")
}

dependencies {
    compileOnly(mn.micronaut.core.processor)

    api(projects.micronautAvroSchemaCommon)
    api(projects.micronautAvroAnnotations)

    testImplementation(mn.micronaut.inject.java.test)
}
