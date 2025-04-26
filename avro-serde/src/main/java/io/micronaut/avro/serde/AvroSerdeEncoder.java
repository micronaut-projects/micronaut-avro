package io.micronaut.avro.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.type.Argument;
import io.micronaut.avro.AvroSchemaSource;
import io.micronaut.avro.model.AvroSchema;
import io.micronaut.avro.serialization.AvroSchemaMapperFactory;
import io.micronaut.serde.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.*;

/***
 * @since 1.0
 */

public class AvroSerdeEncoder implements Encoder {

    private final org.apache.avro.io.Encoder delegate;
    private final boolean isArray;
    private final AvroSchema schema;
    private final AvroSerdeEncoder parent;

    private final Map<String, Runnable> objectBuffer = new TreeMap<>();
    private final List<Runnable> arrayBuffer = new ArrayList<>();
    private String currentKey;

    public AvroSerdeEncoder(org.apache.avro.io.Encoder delegate, boolean isArray, AvroSchema schema, AvroSerdeEncoder parent) {
        this.delegate = delegate;
        this.isArray = isArray;
        this.schema = schema;
        this.parent = parent;
    }

    public AvroSerdeEncoder(org.apache.avro.io.Encoder delegate) {
        this(delegate, false, null, null);
    }

    @Override
    public @NonNull Encoder encodeArray(@NonNull Argument<?> type) throws IOException {
        AvroSerdeEncoder child = new AvroSerdeEncoder(delegate, true, null, this);

        Runnable arrayWriter = () -> {
            try {
                delegate.writeArrayStart();
                if (!child.arrayBuffer.isEmpty()){
                    delegate.setItemCount(child.arrayBuffer.size());
                    for (Runnable r : child.arrayBuffer) {
                        delegate.startItem();
                        r.run();
                    }
                    delegate.writeArrayEnd();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        if (isArray) {
            // If we're inside an array, buffer the nested array as an item
            arrayBuffer.add(() -> {
                try {
                    delegate.startItem();
                    arrayWriter.run();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, arrayWriter);
            currentKey = null;
        } else {
            // This is probably a top-level call
            arrayWriter.run();
        }

        return child;
    }


    @Override
    public @NonNull Encoder encodeObject(@NonNull Argument<?> type) throws IOException {
        Class<?> targetClass = type.getType();
        if (targetClass.isAnnotationPresent(AvroSchemaSource.class)) {
            String schemaName = targetClass.getAnnotation(AvroSchemaSource.class).value();
            String schema = readResource(targetClass.getClassLoader(), schemaName);
            ObjectMapper objectMapper = AvroSchemaMapperFactory.createMapper();
            AvroSchema avroSchema = objectMapper.readValue(schema, AvroSchema.class);
            return new AvroSerdeEncoder(delegate, isArray, avroSchema, this);
        }

        return new AvroSerdeEncoder(delegate, false, null, this);
    }

    @Override
    public void finishStructure() throws IOException {
        if (isArray) {
            delegate.setItemCount(arrayBuffer.size());
            for (Runnable r : arrayBuffer) {
                delegate.startItem();
                r.run();
            }
            delegate.writeArrayEnd();
            arrayBuffer.clear();
        } else {
            if (schema != null) {
                for (AvroSchema.Field field : schema.getFields()) {
                    Runnable fieldEncoder = objectBuffer.get(field.getName());
                    if (fieldEncoder == null) {
                        throw new IOException("Missing field: " + field.getName());
                    }
                    // Check field type
                    System.out.println(field.getType());
                    if (field.getType() instanceof AvroSchema fieldSchema) {
                        if (fieldSchema.getType().equals("array")) {
                            // Handle nested array
                            AvroSerdeEncoder nestedArrayEncoder = new AvroSerdeEncoder(delegate, true, fieldSchema, this);
                            nestedArrayEncoder.finishStructure();
                        } else {
                            // Handle nested object
                            AvroSerdeEncoder nestedObjectEncoder = new AvroSerdeEncoder(delegate, false, fieldSchema, this);
                            nestedObjectEncoder.finishStructure();
                        }
                    } else {
                        // Primitive type
                        fieldEncoder.run();
                    }
                }
            } else {
                // If there's no schema, just finish encoding the object buffer.
                for (Map.Entry<String, Runnable> entry : objectBuffer.entrySet()) {
                    entry.getValue().run();
                }
            }
            objectBuffer.clear();
        }

        if (parent != null && parent.currentKey != null) {
            parent.objectBuffer.put(parent.currentKey, () -> {
                // no-op because this child has already written to the delegate
            });
            parent.currentKey = null;
        }
    }

    @Override
    public void encodeKey(@NonNull String key) throws IOException {
        this.currentKey = key;
    }

    @Override
    public void encodeString(@NonNull String value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> {
            try {
                delegate.writeString(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeBoolean(boolean value) throws IOException {
        validateType(value, AvroSchema.Type.BOOLEAN);
        buffer(() -> {
            try {
                delegate.writeBoolean(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeByte(byte value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> {
            try {
                delegate.writeFixed(new byte[]{value});
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeShort(short value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> {
            try {
                delegate.writeInt(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeChar(char value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> {
            try {
                delegate.writeInt(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeInt(int value) throws IOException {
        validateType(value, AvroSchema.Type.INT);
        buffer(() -> {
            try {
                delegate.writeInt(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeLong(long value) throws IOException {
        validateType(value, AvroSchema.Type.LONG);
        buffer(() -> {
            try {
                delegate.writeLong(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeFloat(float value) throws IOException {
        validateType(value, AvroSchema.Type.FLOAT);
        buffer(() -> {
            try {
                delegate.writeFloat(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeDouble(double value) throws IOException {
        validateType(value, AvroSchema.Type.DOUBLE);
        buffer(() -> {
            try {
                delegate.writeDouble(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeBigInteger(@NonNull BigInteger value) throws IOException {
        validateType(value, AvroSchema.Type.LONG);
        buffer(() -> {
            try {
                delegate.writeBytes(value.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeBigDecimal(@NonNull BigDecimal value) throws IOException {
        validateType(value, AvroSchema.Type.STRING);
        buffer(() -> {
            try {
                delegate.writeString(value.toPlainString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void encodeNull() throws IOException {
        buffer(() -> {
            try {
                delegate.writeNull();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void buffer(Runnable valueWriter) {
        if (isArray) {
            arrayBuffer.add(() -> {
                try {
                    delegate.startItem();
                    valueWriter.run();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } else if (currentKey != null) {
            objectBuffer.put(currentKey, () -> {
                try {
                    valueWriter.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            currentKey = null;
        } else {
            try {
                valueWriter.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String readResource(ClassLoader classLoader, String resourcePath) throws IOException {
        Iterator<URL> specs = classLoader.getResources(resourcePath).asIterator();
        if (!specs.hasNext()) {
            throw new IllegalArgumentException("Could not find resource " + resourcePath);
        }
        URL spec = specs.next();
        BufferedReader reader = new BufferedReader(new InputStreamReader(spec.openStream()));
        StringBuilder result = new StringBuilder();
        String inputLine;
        while ((inputLine = reader.readLine()) != null) {
            result.append(inputLine).append("\n");
        }
        reader.close();
        return result.toString();
    }

    private void validateType(Object value, AvroSchema.Type expectedType) throws IOException {
        if (value == null) {
            return; // null is allowed — Avro handles union with null
        }

        boolean valid;
        switch (expectedType) {
            case STRING -> valid = value instanceof String;
            case BOOLEAN -> valid = value instanceof Boolean;
            case INT -> valid = value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof Character;
            case LONG -> valid = value instanceof Long;
            case FLOAT -> valid = value instanceof Float;
            case DOUBLE -> valid = value instanceof Double;
            case BYTES, FIXED -> valid = value instanceof byte[] || value instanceof Byte[];
            case NULL -> valid = false;
            case ARRAY -> valid = value instanceof Collection;
            case RECORD -> valid = value instanceof Map || value instanceof Record;
            case ENUM -> valid = value instanceof Enum<?>;
            default -> throw new IOException("Unknown Avro type: " + expectedType);
        }

        if (!valid) {
            throw new IOException("Invalid value type. Expected: " + expectedType + ", but got: " + value.getClass().getSimpleName());
        }
    }

}
