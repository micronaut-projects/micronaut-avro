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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that loads the Avro schema from the resources' folder.
 * This annotation can be applied to classes.
 *
 *  <p>The value can support multiple source formats using the following prefixes:
 *  <ul>
 *       <li><b>classpath:</b> Load from the application's classpath (e.g., <code>classpath:schema/salamander-schema.avsc</code>)</li>
 *       <li><b>file:</b> Load from a file on the filesystem (e.g., <code>file:/opt/config/salamander-schema.avsc</code>)</li>
 *       <li><b>url:</b> Load from a remote or local URL (e.g., <code>url:https://example.com/schema.avsc</code>)</li>
 *   </ul>
 *
 *   <p>If no prefix is provided, it may default to classpath-based loading depending on the loader implementation.
 *
 * @since 1.0
 * @author Ali Linaboui
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AvroSchemaSource {

    /**
     * Specifies the location of the Avro schema file to be loaded.
     * The location may include one of the following prefixes:
     * <ul>
     *     <li><code>classpath:</code></li>
     *     <li><code>file:</code></li>
     *     <li><code>url:</code></li>
     * </ul>
     *
     * @return the schema resource path with optional prefix.
     */
    String value();
}
