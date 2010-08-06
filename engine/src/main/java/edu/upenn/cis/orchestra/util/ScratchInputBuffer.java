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


public class ScratchInputBuffer extends InputBuffer {
	private byte[] data;
	private int pastEnd = -1, pos = -1;
	public ScratchInputBuffer() {
	}

	public ScratchInputBuffer(byte[] data) {
		this(data,0,data.length);
	}

	public ScratchInputBuffer(byte[] data, int offset, int length) {
		reset(data, offset, length);
	}

	public void reset(byte[] data) {
		reset(data, 0, data.length);
	}

	public void reset(byte[] data, int offset, int length) {
		this.data = data;
		this.pos = offset;
		this.pastEnd = offset + length;
	}

	@Override
	public boolean readBoolean() {
		checkValid(1);
		if (data[pos++] == 0) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public byte[] readBytes(int length) {
		byte[] retval = new byte[length];
		System.arraycopy(data, pos, retval, 0, length);
		pos += length;
		return retval;
	}

	@Override
	public int readInt() {
		checkValid(IntType.bytesPerInt);
		int retval = IntType.getValFromBytes(data, pos);
		pos += IntType.bytesPerInt;
		return retval;
	}

	public short readShort() {
		checkValid(2);
		int value = ((int) (data[pos++] & 0xFF)) << 8;
		value |= ((int) (data[pos++] & 0xFF));
		return (short) value;
	}

	@Override
	public long readLong() {
		checkValid(LongType.bytesPerLong);
		long retval = IntType.getValFromBytes(data, pos);
		pos += LongType.bytesPerLong;
		return retval;
	}

	public int remaining() {
		return (pastEnd - pos);
	}

	public boolean finished() {
		return remaining() <= 0;
	}

	@Override
	public ByteArrayWrapper readByteArrayWrapperWithoutCopying(int length) {
		int oldPos = pos;
		pos += length;
		return new ByteArrayWrapper(data, oldPos, length);
	}
	
	@Override
	public byte[] readBytesWithoutCopying(int length) {
		this.lastReadOffset = pos;
		pos += length;
		return data;
	}
	
	private void checkValid(int numBytesToRead) {
		if (pos + numBytesToRead > pastEnd) {
			throw new IllegalStateException("ScratchInputBuffer has only " + remaining() + " bytes to read");
		}
	}
}
