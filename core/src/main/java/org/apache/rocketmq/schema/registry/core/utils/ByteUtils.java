package org.apache.rocketmq.schema.registry.core.utils;

import java.io.*;
import java.nio.ByteBuffer;

public final class ByteUtils {
	public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);
	
	private ByteUtils() {
	}
	
	public static long readUnsignedInt(ByteBuffer buffer) {
		return (long)buffer.getInt() & 4294967295L;
	}
	
	public static long readUnsignedInt(ByteBuffer buffer, int index) {
		return (long)buffer.getInt(index) & 4294967295L;
	}
	
	public static int readUnsignedIntLE(InputStream in) throws IOException {
		return in.read() | in.read() << 8 | in.read() << 16 | in.read() << 24;
	}
	
	public static int readUnsignedIntLE(byte[] buffer, int offset) {
		return buffer[offset] << 0 & 255 | (buffer[offset + 1] & 255) << 8 | (buffer[offset + 2] & 255) << 16 | (buffer[offset + 3] & 255) << 24;
	}
	
	public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
		buffer.putInt(index, (int)(value & 4294967295L));
	}
	
	public static void writeUnsignedInt(ByteBuffer buffer, long value) {
		buffer.putInt((int)(value & 4294967295L));
	}
	
	public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
		out.write(value);
		out.write(value >>> 8);
		out.write(value >>> 16);
		out.write(value >>> 24);
	}
	
	public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
		buffer[offset] = (byte)value;
		buffer[offset + 1] = (byte)(value >>> 8);
		buffer[offset + 2] = (byte)(value >>> 16);
		buffer[offset + 3] = (byte)(value >>> 24);
	}
	
	public static int readUnsignedVarint(ByteBuffer buffer) {
		int value = 0;
		int i = 0;
		
		do {
			byte b;
			if (((b = buffer.get()) & 128) == 0) {
				value |= b << i;
				return value;
			}
			
			value |= (b & 127) << i;
			i += 7;
		} while(i <= 28);
		
		throw illegalVarintException(value);
	}
	
	public static int readUnsignedVarint(DataInput in) throws IOException {
		int value = 0;
		int i = 0;
		
		do {
			byte b;
			if (((b = in.readByte()) & 128) == 0) {
				value |= b << i;
				return value;
			}
			
			value |= (b & 127) << i;
			i += 7;
		} while(i <= 28);
		
		throw illegalVarintException(value);
	}
	
	public static int readVarint(ByteBuffer buffer) {
		int value = readUnsignedVarint(buffer);
		return value >>> 1 ^ -(value & 1);
	}
	
	public static int readVarint(DataInput in) throws IOException {
		int value = readUnsignedVarint(in);
		return value >>> 1 ^ -(value & 1);
	}
	
	public static long readVarlong(DataInput in) throws IOException {
		long value = 0L;
		int i = 0;
		
		do {
			long b;
			if (((b = (long)in.readByte()) & 128L) == 0L) {
				value |= b << i;
				return value >>> 1 ^ -(value & 1L);
			}
			
			value |= (b & 127L) << i;
			i += 7;
		} while(i <= 63);
		
		throw illegalVarlongException(value);
	}
	
	public static long readVarlong(ByteBuffer buffer) {
		long value = 0L;
		int i = 0;
		
		do {
			long b;
			if (((b = (long)buffer.get()) & 128L) == 0L) {
				value |= b << i;
				return value >>> 1 ^ -(value & 1L);
			}
			
			value |= (b & 127L) << i;
			i += 7;
		} while(i <= 63);
		
		throw illegalVarlongException(value);
	}
	
	public static double readDouble(DataInput in) throws IOException {
		return in.readDouble();
	}
	
	public static double readDouble(ByteBuffer buffer) {
		return buffer.getDouble();
	}
	
	public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
		while((long)(value & -128) != 0L) {
			byte b = (byte)(value & 127 | 128);
			buffer.put(b);
			value >>>= 7;
		}
		
		buffer.put((byte)value);
	}
	
	public static void writeUnsignedVarint(int value, DataOutput out) throws IOException {
		while((long)(value & -128) != 0L) {
			byte b = (byte)(value & 127 | 128);
			out.writeByte(b);
			value >>>= 7;
		}
		
		out.writeByte((byte)value);
	}
	
	public static void writeVarint(int value, DataOutput out) throws IOException {
		writeUnsignedVarint(value << 1 ^ value >> 31, out);
	}
	
	public static void writeVarint(int value, ByteBuffer buffer) {
		writeUnsignedVarint(value << 1 ^ value >> 31, buffer);
	}
	
	public static void writeVarlong(long value, DataOutput out) throws IOException {
		long v;
		for(v = value << 1 ^ value >> 63; (v & -128L) != 0L; v >>>= 7) {
			out.writeByte((int)v & 127 | 128);
		}
		
		out.writeByte((byte)((int)v));
	}
	
	public static void writeVarlong(long value, ByteBuffer buffer) {
		long v;
		for(v = value << 1 ^ value >> 63; (v & -128L) != 0L; v >>>= 7) {
			byte b = (byte)((int)(v & 127L | 128L));
			buffer.put(b);
		}
		
		buffer.put((byte)((int)v));
	}
	
	public static void writeDouble(double value, DataOutput out) throws IOException {
		out.writeDouble(value);
	}
	
	public static void writeDouble(double value, ByteBuffer buffer) {
		buffer.putDouble(value);
	}
	
	public static int sizeOfUnsignedVarint(int value) {
		int leadingZeros = Integer.numberOfLeadingZeros(value);
		int leadingZerosBelow38DividedBy7 = (38 - leadingZeros) * 74899 >>> 19;
		return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
	}
	
	public static int sizeOfVarint(int value) {
		return sizeOfUnsignedVarint(value << 1 ^ value >> 31);
	}
	
	public static int sizeOfVarlong(long value) {
		long v = value << 1 ^ value >> 63;
		int leadingZeros = Long.numberOfLeadingZeros(v);
		int leadingZerosBelow70DividedBy7 = (70 - leadingZeros) * 74899 >>> 19;
		return leadingZerosBelow70DividedBy7 + (leadingZeros >>> 6);
	}
	
	private static IllegalArgumentException illegalVarintException(int value) {
		throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, converted value: " + Integer.toHexString(value));
	}
	
	private static IllegalArgumentException illegalVarlongException(long value) {
		throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, converted value: " + Long.toHexString(value));
	}
}

