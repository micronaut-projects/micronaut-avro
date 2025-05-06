package io.micronaut.avro.visitor

import io.micronaut.core.io.ResourceLoader;
import io.micronaut.serde.ObjectMapper;
import io.micronaut.annotation.processing.test.AbstractTypeElementSpec
import io.micronaut.avro.model.AvroSchema
import io.micronaut.avro.visitor.context.AvroSchemaContext;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets;

abstract class AbstractAvroSchemaSpec extends AbstractTypeElementSpec {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAvroSchemaSpec.class);
    private final ResourceLoader resourceLoader;

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
        ObjectMapper objectMapper = ObjectMapper.getDefault();
        AvroSchema avroSchema = objectMapper.readValue(avro, AvroSchema.class)
        return avroSchema
    }

    private String readResource(ClassLoader classLoader, String resourcePath) throws IOException {
        Optional<URL> url = resourceLoader.getResource(resourcePath);
        if (url.isPresent()) {
            try (InputStream inputStream = url.get().openStream()) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)
            }
        } else {
            throw new IOException("Resource not found: " + resourcePath);
        }
    }
}
