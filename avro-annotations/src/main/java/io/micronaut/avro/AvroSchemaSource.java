package io.micronaut.avro;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that loads the Avro schema from the resources' folder.
 * This annotation can be applied to classes.
 * @since 1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AvroSchemaSource {

    /**
     * Specifies the name of the Avro schema file to be loaded.
     * like ("salamander-schema.avsc")
     *
     * @return the name of the Avro schema file.
     */
    String value();
}
