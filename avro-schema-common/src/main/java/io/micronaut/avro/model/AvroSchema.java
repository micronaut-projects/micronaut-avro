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
package io.micronaut.avro.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.serde.annotation.Serdeable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Represents an Avro schema, which defines the structure of data in Avro format.
 *
 * @author Ali Linaboui
 * @since 1.0
 */
@Internal
@Serdeable
public final class AvroSchema {

    private Object type;
    private String name;
    private String namespace;
    @JsonIgnore
    private String fullName;
    private String doc;
    private List<String> aliases;
    @JsonIgnore
    private boolean unsupported;
    @JsonIgnore
    private boolean refType;
    /**
     * For record Type.
     */
    private List<Field> fields;

    /**
     * For enum Type.
     */
    private List<String> symbols;

    /**
     * For array Type.
     */
    private Object items;

    /**
     * For map Type.
     */
    private Object values;

    /**
     * For fixed type.
     */
    private Integer size;

    /**
     * For union type.
     */
    private List<AvroSchema> types;

    /**
     * Default value.
     */
    private Object defaultValue;

    /**
     * A logical type is an Avro primitive or complex type with extra attributes to represent a derived type.
     */
    private LogicalType logicalType;

    /**
     * for Decimal logical type.
     * representing the scale (optional). If not specified the scale is 0.
     */
    private Integer scale;

    /**
     * for Decimal logical type.
     * representing the (maximum) precision of decimals stored in this type (required).
     */
    private Integer precision;

    private String javaClass;

    /**
     * Creates an empty Avro schema.
     */
    public AvroSchema() { }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public Object getType() {
        return type;
    }

    public void setType(Object type) {
        this.type = type;
    }

    public AvroSchema addType(AvroSchema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        if (this.types == null) {
            this.types = new ArrayList<>();
        }
        this.types.add(schema);
        return this;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    public List<String> getAliases() {
        return aliases;
    }

    public void setAliases(List<String> aliases) {
        this.aliases = aliases;
    }

    public List<Field> getFields() {
        return fields;
    }

    public AvroSchema setFields(List<Field> fields) {
        this.fields = fields;
        return this;
    }

    public AvroSchema addField(Field field) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(field);
        return this;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public AvroSchema setSymbols(List<String> symbols) {
        this.symbols = symbols;
        return this;
    }

    public void addSymbol(String symbol) {
        if (symbols == null) {
            symbols = new ArrayList<>();
        }
        symbols.add(symbol);
    }

    public boolean isRefType() {
        return refType;
    }

    public void setRefType(boolean refType) {
        this.refType = refType;
    }

    public Object getItems() {
        return items;
    }

    public AvroSchema setItems(Object items) {
        this.items = items;
        return this;
    }

    public Object getValues() {
        return values;
    }

    public void setValues(Object values) {
        this.values = values;
    }

    public Integer getSize() {
        return size;
    }

    public AvroSchema setSize(Integer size) {
        this.size = size;
        return this;
    }

    public List<AvroSchema> getTypes() {
        return types;
    }

    public AvroSchema setTypes(List<AvroSchema> types) {
        this.types = types;
        return this;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public AvroSchema setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }

    public void setLogicalType(LogicalType logicalType) {
        this.logicalType = logicalType;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public boolean isUnsupported() {
        return unsupported;
    }

    public void setUnsupported(boolean unsupported) {
        this.unsupported = unsupported;
    }

    /**
     * Represents a field in an Avro schema.
     */
    public static final class Field {

        private String name;
        private String doc;
        private Object type;
        private Order order;
        private List<String> aliases;
        private Object defaultValue;

        public Field() { }

        public String getName() {
            return name;
        }

        public Field setName(String name) {
            this.name = name;
            return this;
        }

        public String getDoc() {
            return doc;
        }

        public void setDoc(String doc) {
            this.doc = doc;
        }

        public Object getType() {
            return type;
        }

        public void setType(Object type) {
            this.type = type;
        }

        public Order getOrder() {
            return order;
        }

        public Field setOrder(Order order) {
            this.order = order;
            return this;
        }

        public List<String> getAliases() {
            return aliases;
        }

        public void setAliases(List<String> aliases) {
            this.aliases = aliases;
        }

        public Field addAlias(String alias) {
            if (aliases == null) {
                aliases = new ArrayList<>();
            }
            aliases.add(alias);
            return this;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public Field setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }
    }

    /**
     * Enumerates the possible orders of a record field.
     */
    public enum Order {
        ASCENDING,
        DESCENDING,
        IGNORE;
    }

    /**
     * The type of schema representing Avro's primitive and complex types.
     */
    public enum Type {
        /** A Null Value. */
        NULL,
        /** A  "true" or "false" Value. */
        BOOLEAN,
        /** A 32-bit singed integer. */
        INT,
        /** A 64-bit signed integer. */
        LONG,
        /** A single precision (32-bit) IEEE 754 floating-point number. */
        FLOAT,
        /** A double precision (64-bit) IEEE 754 floating-point number. */
        DOUBLE,
        /** A sequence of 8-bit unsigned bytes. */
        BYTES,
        /** A Unicode character sequence. */
        STRING,
        /** A record, a named group of fields. */
        RECORD,
        /** An enumeration, a set of named values. */
        ENUM,
        /** An ordered list of instances. */
        ARRAY,
        /** A map of values, with string keys. */
        MAP,
        /** A union of schemas.  */
        UNION,
        /** An integer, specifying the number of bytes per value. */
        FIXED;

        @JsonValue
        public String value() {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        static Type fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        }

    }

    /**
     * Logical types that can be applied to Avro schemas.
     */
    public enum LogicalType {
        DECIMAL,
        DATE,
        TIME_MILLIS,
        TIME_MICROS,
        TIMESTAMP_MILLIS,
        TIMESTAMP_MICROS,
        DURATION,
        UUID;

        @JsonValue
        public String value() {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        static LogicalType fromString(String value) {
            return LogicalType.valueOf(value.toUpperCase(Locale.ENGLISH));
        }
    }
}
