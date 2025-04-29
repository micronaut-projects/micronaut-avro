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
package io.micronaut.avro.visitor.context;

import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ast.ClassElement;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.avro.model.AvroSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A context with configuration for the Avro schema generation.
 * @since 1.0
 *
 * @param outputLocation The location where Avro schemas will be generated inside the build {@code META-INF/} directory.
 * @param useDecimalLogicalType Whether to use logical type 'decimal' for BigDecimal fields.
 * @param baseUrl The base URI to be used for schemas.
 * @param useTimeMillisLogicalType Whether to use logical type 'time-millis' for time fields.
 * @param useTimestampMillisLogicalType Whether to use logical type 'timestamp-millis' for timestamp fields.
 * @param useLogicalTypes Whether to use logical types for appropriate types (UUID, Date, etc.).
 * @param useEnumSymbols Whether to use enum symbols (true) or string types (false) for Java enums.
 * @param strictMode Whether to generate schemas in strict mode.
 *                   In strict mode, fields will be required by default unless annotated as nullable.
 * @param createdSchemasByType A cache of created schemas
 * @param currentOriginatingElements The originating elements for the current schema
 */
public record AvroSchemaContext(
        String outputLocation,
        boolean useDecimalLogicalType,
        String baseUrl,
        boolean useTimeMillisLogicalType,
        boolean useTimestampMillisLogicalType,
        boolean useLogicalTypes,
        boolean useEnumSymbols,
        boolean strictMode,
        Map<String, AvroSchema> createdSchemasByType,
        List<ClassElement> currentOriginatingElements
) {

    public static final String AVRO_SCHEMA_CONTEXT_PROPERTY = "io.micronaut.avroschema";

    public static final String PARAMETER_PREFIX = VisitorContext.MICRONAUT_BASE_OPTION_NAME + ".avroschema.";
    public static final String OUTPUT_LOCATION_PARAMETER = PARAMETER_PREFIX + "outputLocation";
    public static final String NAMESPACE_PARAMETER = PARAMETER_PREFIX + "namespace";
    public static final String USE_DECIMAL_LOGICAL_TYPE_PARAMETER = PARAMETER_PREFIX + "useDecimalLogicalType";
    public static final String USE_TIME_MILLIS_LOGICAL_TYPE_PARAMETER = PARAMETER_PREFIX + "useTimeMillisLogicalType";
    public static final String USE_TIMESTAMP_MILLIS_LOGICAL_TYPE_PARAMETER = PARAMETER_PREFIX + "useTimestampMillisLogicalType";
    public static final String USE_LOGICAL_TYPES_PARAMETER = PARAMETER_PREFIX + "useLogicalTypes";
    public static final String USE_ENUM_SYMBOLS_PARAMETER = PARAMETER_PREFIX + "useEnumSymbols";
    public static final String STRICT_MODE_PARAMETER = PARAMETER_PREFIX + "strictMode";
    public static final String BASE_URI_PARAMETER = PARAMETER_PREFIX + "baseUri";
    private static final String DEFAULT_BASE_URL = "http://localhost:8080/avro-schemas";

    public static final String DEFAULT_OUTPUT_LOCATION = "avro-schemas";
    public static final boolean DEFAULT_USE_DECIMAL_LOGICAL_TYPE = true;
    public static final boolean DEFAULT_USE_TIME_MILLIS_LOGICAL_TYPE = true;
    public static final boolean DEFAULT_USE_TIMESTAMP_MILLIS_LOGICAL_TYPE = true;
    public static final boolean DEFAULT_USE_LOGICAL_TYPES = true;
    public static final boolean DEFAULT_USE_ENUM_SYMBOLS = true;
    public static final boolean DEFAULT_STRICT_MODE = false;

    public static Set<String> getParameters() {
        return Set.of(
                OUTPUT_LOCATION_PARAMETER,
                NAMESPACE_PARAMETER,
                USE_DECIMAL_LOGICAL_TYPE_PARAMETER,
                USE_TIME_MILLIS_LOGICAL_TYPE_PARAMETER,
                USE_TIMESTAMP_MILLIS_LOGICAL_TYPE_PARAMETER,
                USE_LOGICAL_TYPES_PARAMETER,
                USE_ENUM_SYMBOLS_PARAMETER,
                STRICT_MODE_PARAMETER
        );
    }

    public static AvroSchemaContext createDefault(Map<String, String> options) {
        String outputLocation = options.getOrDefault(OUTPUT_LOCATION_PARAMETER, DEFAULT_OUTPUT_LOCATION);
        String baseUrl = options.getOrDefault(BASE_URI_PARAMETER, DEFAULT_BASE_URL);
        boolean useDecimalLogicalType = options.getOrDefault(USE_DECIMAL_LOGICAL_TYPE_PARAMETER,
                String.valueOf(DEFAULT_USE_DECIMAL_LOGICAL_TYPE)).equals(StringUtils.TRUE);
        boolean useTimeMillisLogicalType = options.getOrDefault(USE_TIME_MILLIS_LOGICAL_TYPE_PARAMETER,
                String.valueOf(DEFAULT_USE_TIME_MILLIS_LOGICAL_TYPE)).equals(StringUtils.TRUE);
        boolean useTimestampMillisLogicalType = options.getOrDefault(USE_TIMESTAMP_MILLIS_LOGICAL_TYPE_PARAMETER,
                String.valueOf(DEFAULT_USE_TIMESTAMP_MILLIS_LOGICAL_TYPE)).equals(StringUtils.TRUE);
        boolean useLogicalTypes = options.getOrDefault(USE_LOGICAL_TYPES_PARAMETER,
                String.valueOf(DEFAULT_USE_LOGICAL_TYPES)).equals(StringUtils.TRUE);
        boolean useEnumSymbols = options.getOrDefault(USE_ENUM_SYMBOLS_PARAMETER,
                String.valueOf(DEFAULT_USE_ENUM_SYMBOLS)).equals(StringUtils.TRUE);
        boolean strictMode = options.getOrDefault(STRICT_MODE_PARAMETER,
                String.valueOf(DEFAULT_STRICT_MODE)).equals(StringUtils.TRUE);

        return new AvroSchemaContext(
                outputLocation,
                useDecimalLogicalType,
                baseUrl,
                useTimeMillisLogicalType,
                useTimestampMillisLogicalType,
                useLogicalTypes,
                useEnumSymbols,
                strictMode,
                new HashMap<>(),
                new ArrayList<>()
        );
    }
}
