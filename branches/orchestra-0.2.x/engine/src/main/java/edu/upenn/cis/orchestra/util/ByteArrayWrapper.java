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

public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
	private static final int[] hashMultipliers = {17, 37,  89, 157, 293, 601, 1201, 2411, 4801, 9601,  20011, 29, 59, 113, 229, 461, 919, 1847, 3677, 7193, 14407, 29009};
	public final byte[] array;
	public final int offset, length;
	private int hashCode = 0;

	/**
	 * Build a ByteArrayWrapper from the specified array (which is not copied)
	 * 
	 * @param array
	 */
	public ByteArrayWrapper(byte[] array) {
		this.array = array;
		this.offset = 0;
		this.length = array.length;
	}
	
	/**
	 * Build a ByteArrayWrapper from the specified subsequence of an array (which
	 * is not copied)
	 * 
	 * @param array
	 * @param offset
	 * @param length
	 */
	public ByteArrayWrapper(byte[] array, int offset, int length) {
		this.array = array;
		this.offset = offset;
		this.length = length;
	}

	public boolean equals(Object o) {
		ByteArrayWrapper baw = (ByteArrayWrapper) o;
		if (baw.length != length) {
			return false;
		}
		for (int i = 0; i < length; ++i) {
			if (array[offset +i] != baw.array[baw.offset + i]) {
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		if (hashCode == 0) {
			hashCode = hashCode(array,offset,length);
		}
		return hashCode;
	}
	
	public static int hashCode(byte[] array, int offset, int length) {
		final int max;
		if (length > hashMultipliers.length) {
			max = hashMultipliers.length;
		} else {
			max = length;
		}
		int hashCode = 0;
        for (int i = 0; i < max; ++i) {
        	hashCode += hashMultipliers[i] * array[offset + i];
        }
		return hashCode;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		if (length != 0) {
			sb.append(array[offset]);
		}
		for (int i = offset + 1; i < offset + length; ++i) {
			sb.append(',');
			sb.append(array[i]);
		}
		sb.append(']');
		return sb.toString();
	}

	public int compareTo(ByteArrayWrapper baw) {
		final int minLen = length < baw.length ? length : baw.length;

		for (int i = 0; i < minLen; ++i) {
			int diff = ((int) array[offset + i]) - baw.array[baw.offset + i];
			if (diff != 0) {
				return diff;
			}
		}

		if (length < baw.length) {
			return -1;
		} else if (baw.length > length) {
			return 1;
		} else {
			return 0;
		}
	}
	
	/**
	 * Copy this byte array into another byte array. It is assumed that the remaining
	 * space in the other byte array is large enough to hold this byte array
	 * 
	 * @param dest		An array to copy this byte array into
	 * @param destOffset	The offset in <code>dest</code> at which to put the data
	 */
	public void copyInto(byte[] dest, int destOffset) {
		for (int i = 0; i < length; ++i) {
			dest[destOffset + i] = array[offset + i];
		}
	}
	
	public byte[] getCopy() {
		byte[] retval = new byte[length];
		System.arraycopy(array, offset, retval, 0, retval.length);
		return retval;
	}	
}