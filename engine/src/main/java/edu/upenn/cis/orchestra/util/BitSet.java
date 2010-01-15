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

public class BitSet {
	private static final int BITS_PER_BYTE = 8;
	private final byte[] data;
	
	public BitSet(int size) {
		data = new byte[numBytes(size)];
	}
	
	public static int numBytes(int size) {
		int numFields = size / BITS_PER_BYTE;
		if (numFields * BITS_PER_BYTE < size) {
			++numFields;
		}
		return numFields;
	}
	
	public BitSet(byte[] data) {
		this.data = new byte[data.length];
		System.arraycopy(data, 0, this.data, 0, data.length);
	}
	
	public byte[] getData() {
		byte[] retval = new byte[data.length];
		System.arraycopy(data, 0, retval, 0, retval.length);
		return retval;
	}
	
	public void set(int pos) {
		data[getField(pos)] |= getMask(pos);
	}
	
	public void clear(int pos) {
		data[getField(pos)] &= (~ getMask(pos));
	}
	
	public boolean get(int pos) {
		return (data[getField(pos)] & getMask(pos)) != 0;
	}
	
	private static int getField(int pos) {
		return pos >> 3;
	}
	
	private static byte getMask(int pos) {
		byte mask = 1;
		mask <<= (pos & 7);
		return mask;
	}
	
	public static boolean getField(int pos, byte[] data, int offset) {
		return (data[getField(pos) + offset] & getMask(pos)) != 0;
	}
	
	public static void setField(int pos, byte[] data, int offset) {
		data[getField(pos) + offset] |= getMask(pos);
	}

	public static void clearField(int pos, byte[] data, int offset) {
		data[getField(pos) + offset] &= (~ getMask(pos));
	}
}
