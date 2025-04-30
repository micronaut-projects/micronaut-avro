package io.micronaut.avro.visitor;

import io.micronaut.serde.ObjectMapper;
import io.micronaut.annotation.processing.test.AbstractTypeElementSpec
import io.micronaut.avro.model.AvroSchema
import io.micronaut.avro.serialization.AvroSchemaMapperFactory;
import io.micronaut.avro.visitor.context.AvroSchemaContext;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractAvroSchemaSpec extends AbstractTypeElementSpec {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAvroSchemaSpec.class);

    protected AvroSchema buildAvroSchema(String className, String schemaName, @Language("java") String cls, String... parameters) {
        return buildAvroSchema(className, schemaName, cls.formatted(parameters), null)
    }

    protected AvroSchema buildAvroSchema(String className, String schemaName, @Language("java") String cls, Map<String, String> contextOptions) {
        for (String parameter: AvroSchemaContext.getParameters()) {
            System.clearProperty(parameter)
        }
        if (contextOptions != null) {
            contextOptions.each{ System.setProperty(AvroSchemaContext.PARAMETER_PREFIX +  it.key, it.value) }
        }
        ClassLoader classLoader = buildClassLoader(className, cls)
        String avro = readResource(classLoader, "META-INF/avro-schemas/" + schemaName + "-schema.avsc")
        LOGGER.info("Read AVRO schema: ")
        LOGGER.info(avro)
        ObjectMapper objectMapper = AvroSchemaMapperFactory.createMapper()
        AvroSchema avroSchema = objectMapper.readValue(avro, AvroSchema.class)
        return avroSchema
    }

    protected String readResource(ClassLoader classLoader, String resourcePath) {
        Iterator<URL> specs = classLoader.getResources(resourcePath).asIterator()
        if (!specs.hasNext()) {
            throw new IllegalArgumentException("Could not find resource " + resourcePath)
        }
        URL spec = specs.next()
        BufferedReader reader = new BufferedReader(new InputStreamReader(spec.openStream()))
        StringBuilder result = new StringBuilder()
        String inputLine
        while ((inputLine = reader.readLine()) != null) {
            result.append(inputLine).append("\n")
        }
        reader.close()
        return result.toString()
    }
}
