/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.util;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.LongType;



public class ScratchOutputBuffer extends OutputBuffer implements WriteableByteArray {
	private byte[] data;
	private int nextWrite = 0;
	
	public ScratchOutputBuffer() {
		this(256);
	}
	
	public ScratchOutputBuffer(int initialSize) {
		if (initialSize < 8) {
			initialSize = 8;
		}
		data = new byte[initialSize];
	}
	
	@Override
	public void writeBoolean(boolean bool) {
		prepareForWrite(1);
		data[nextWrite++] = (byte) (bool ? 0 : 1);
	}

	@Override
	public void writeBytesNoLength(byte[] bytes) {
		writeBytesNoLength(bytes, 0, bytes.length);
	}

	@Override
	public void writeBytesNoLength(byte[] bytes, int offset, int length) {
		prepareForWrite(length);
		System.arraycopy(bytes, offset, data, nextWrite, length);
		nextWrite += length;
	}

	@Override
	public void writeInt(int v) {
		prepareForWrite(4);
		IntType.putBytes(v, data, nextWrite);
		nextWrite += IntType.bytesPerInt;
	}

	@Override
	public void writeLong(long l) {
		prepareForWrite(8);
		LongType.putBytes(l, data, nextWrite);
		nextWrite += LongType.bytesPerLong;
	}

	@Override
	public void writeShort(short s) {
		prepareForWrite(2);
		data[nextWrite++] = (byte) (s >>> 8);
		data[nextWrite++] = (byte) s;
	}

	private void prepareForWrite(int length) {
		if (data.length - nextWrite < length) {
			int newLength = 2 * (data.length);
			if (newLength - (data.length) < length) {
				newLength =  data.length + length;
			}
			byte[] newData = new byte[newLength];
			System.arraycopy(data, 0, newData, 0, nextWrite);
			data = newData;
		}
	}
	
	public int length() {
		return nextWrite;
	}

	public ScratchInputBuffer getInputBuffer() {
		return new ScratchInputBuffer(data, 0, nextWrite);
	}
	
	public void writeContents(OutputBuffer out) {
		out.writeBytesNoLength(data, 0, nextWrite);
	}
	
	public void reset() {
		data = new byte[data.length];
		nextWrite = 0;
	}
	
	public byte[] getData() {
		byte[] retval = new byte[nextWrite];
		System.arraycopy(data, 0, retval, 0, nextWrite);
		return retval;
	}
	
	public ByteArrayWrapper getDataNoCopy() {
		return new ByteArrayWrapper(data, 0, nextWrite);
	}
	
	public int getWriteableByteArrayOffset(int length, boolean writeLength) {
		if (writeLength) {
			writeInt(length);
		}
		prepareForWrite(length);
		int retval = nextWrite;
		nextWrite += length;
		return retval;
	}
	
	public byte[] getWriteableByteArray() {
		return data;
	}
	
	int mark = -1;
	
	public void mark() {
		mark = nextWrite;
	}
	
	public void resetToMark() {
		if (mark == -1) {
			throw new IllegalStateException("Mark is not set");
		}
		nextWrite = mark;
		mark = -1;
	}
	
	public void clearMark() {
		mark = -1;
	}
}
