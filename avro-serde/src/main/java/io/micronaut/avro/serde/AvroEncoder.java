package io.micronaut.avro.serde;

import io.micronaut.core.annotation.Order;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Avro implementation of {@link org.apache.avro.io.Encoder}
 */
public class AvroEncoder extends Encoder {

    private final OutputStream out;

    public AvroEncoder(OutputStream out) {
        this.out = out;
    }

    @Override
    public void writeArrayStart() throws IOException {
    }

    @Override
    public void setItemCount(long itemCount) throws IOException {
        writeLong(itemCount);
    }

    @Override
    public void startItem() throws IOException {
    }

    @Override
    public void writeArrayEnd() throws IOException {
        writeLong(0); // Signal end of array
    }

    @Override
    public void writeString(String value) throws IOException {
        if (value == null) {
            writeLong(0);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            writeLong(bytes.length);
            out.write(bytes);
        }
    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        writeInt(len);
        out.write(bytes, start, len);
    }

    @Override
    public void writeBytes(ByteBuffer bytes) throws IOException {
        int len = bytes.remaining();
        writeInt(len);
        byte[] array = new byte[len];
        bytes.get(array);
        out.write(array);
    }

    @Override
    public void writeFixed(byte[] bytes, int start, int len) throws IOException {
        out.write(bytes, start, len);
    }

    @Override
    public void writeEnum(int e) throws IOException {
        writeInt(e);
    }

    @Override
    public void writeMapStart() throws IOException {
    }

    @Override
    public void writeMapEnd() throws IOException {
        writeLong(0);
    }

    @Override
    public void writeIndex(int unionIndex) throws IOException {
        writeInt(unionIndex);
    }

    @Override
    public void writeNull() throws IOException {
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        out.write(b ? 1 : 0);
    }

    @Override
    @Order(1)
    public void writeInt(int value) throws IOException {
        writeVarInt((value << 1) ^ (value >> 31));
    }

    @Override
    public void writeLong(long value) throws IOException {
        writeVarLong((value << 1) ^ (value >> 63));
    }

    @Override
    public void writeFloat(float f) throws IOException {
        int bits = Float.floatToIntBits(f);
        out.write(new byte[]{
            (byte) bits,
            (byte) (bits >>> 8),
            (byte) (bits >>> 16),
            (byte) (bits >>> 24)
        });
    }

    @Override
    public void writeDouble(double d) throws IOException {
        long bits = Double.doubleToLongBits(d);
        out.write(new byte[]{
            (byte) bits,
            (byte) (bits >>> 8),
            (byte) (bits >>> 16),
            (byte) (bits >>> 24),
            (byte) (bits >>> 32),
            (byte) (bits >>> 40),
            (byte) (bits >>> 48),
            (byte) (bits >>> 56)
        });
    }

    @Override
    public void writeString(Utf8 utf8) throws IOException {
        if (utf8 == null) {
            writeInt(0);
        } else {
            byte[] bytes = utf8.getBytes();
            writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private void writeVarInt(int value) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write(value);
    }

    private void writeVarLong(long value) throws IOException {
        while ((value & ~0x7FL) != 0) {
            out.write(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write((int) value);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }


}
