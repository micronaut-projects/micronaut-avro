/*
 * Copyright 2017-2025 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.avro;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * An annotation that signifies that an Avro schema should be created for the object.
 * The Avro schema will attempt to reflect the way this object would be serialized.
 *
 * @since 1.0
 * @author Ali Linaboui
 */
@Target({ ElementType.TYPE, ElementType.FIELD })
public @interface Avro {

    /**
     * The name of the Avro schema.
     * By default, the class name will be used.
     *
     * @return The name of the schema.
     */
    String name() default "";

    /**
     * The documentation of the Avro schema.
     * A JSON string providing documentation to the user of this schema.
     * By default, "NONE".
     *
     * @return The documentation string of the schema.
     */
    String doc() default "";

    /**
     * The namespace of the Avro schema.
     * A JSON string that qualifies the name.
     *
     * @return The namespace of the schema.
     */
    String namespace() default "";

    /**
     * Aliases for the schema.
     *
     * @return The aliases for the schema.
     */
    String[] aliases() default {};
}
