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
package io.micronaut.avro.serde.loader;

import io.micronaut.core.io.ResourceLoader;
import io.micronaut.serde.ObjectMapper;
import io.micronaut.avro.model.AvroSchema;
import io.micronaut.core.annotation.Internal;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

/**
 *  Helper class used to load Avro schemas from various locations.
 *
 * @since 1.0
 * @author Ali Linaboui
 */
@Internal
public class AvroSchemaLoader {

    private static final String CLASSPATH_PREFIX = "classpath:";
    private static final String FILE_PREFIX = "file:";
    private static final String URL_PREFIX = "url:";

    /**
     * Loads an Avro schema from the specified location.
     * The location string can be prefixed with one of the following:
     * <ul>
     *   <li><code>classpath:</code> - loads the schema from the classpath</li>
     *   <li><code>file:</code> - loads the schema from a file on the local file system</li>
     *   <li><code>url:</code> - loads the schema from a URL</li>
     * </ul>
     *
     * @param location the location of the Avro schema (with optional prefix)
     * @return the loaded Avro schema as a string
     * @throws IOException if an I/O error occurs while loading the schema
     */
    public static String loadSchema(String location, ClassLoader classLoader) throws IOException {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("Location cannot be null or empty");
        }

        if (location.startsWith(CLASSPATH_PREFIX)) {
            return loadFromClasspath(location.substring(CLASSPATH_PREFIX.length()), classLoader);
        } else if (location.startsWith(FILE_PREFIX)) {
            return loadFromFile(location.substring(FILE_PREFIX.length()));
        } else if (location.startsWith(URL_PREFIX)) {
            return loadFromUrl(location.substring(URL_PREFIX.length()));
        } else {
            // Default to classpath loading if no prefix is provided
            return loadFromClasspath(location, classLoader);
        }
    }

    private static String loadFromClasspath(String resourcePath, ClassLoader classLoader) throws IOException {
        try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String loadFromFile(String filePath) throws IOException {
        return Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
    }

    private static String loadFromUrl(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        try (InputStream inputStream = url.openStream()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Loads an Avro schema from the specified location and deserializes it into an AvroSchema object.
     *
     * @param location the location of the Avro schema (with optional prefix)
     * @param objectMapper the ObjectMapper to use for deserialization
     * @return the loaded Avro schema
     * @throws IOException if an I/O error occurs while loading or deserializing the schema
     */
    public static AvroSchema load(String location, ObjectMapper objectMapper, ClassLoader classLoader) throws IOException {
        String schemaStr = loadSchema(location, classLoader);
        return objectMapper.readValue(schemaStr, AvroSchema.class);
    }
}

