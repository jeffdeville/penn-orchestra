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

import java.util.Arrays;

import org.apache.log4j.Logger;

public class BloomFilter<T> {

	public interface Hasher<T> {
		int hashCode1(T val);
		int hashCode2(T val);
	}

	private final static int sizes[] = { 31, 61, 89, 127, 251, 509, 631, 797, 1021,
		2039, 4093, 8191}; 

	private final int[] filter;
	private final int numBits;
	private final int numFuncs;
	private final Hasher<T> hasher;

	private final static double LOG_TWO = Math.log(2);

	private static int computeNumFuncs(int numBits, int numElements) {
		if (numElements <= 0 || numBits <= 0) {
			throw new IllegalArgumentException("Both filter size and expected number of elements must be positive");
		}
		return (int) Math.ceil(((double) numBits) * LOG_TWO / numElements);
	}

	public BloomFilter(int numBits, int numElements, Hasher<T> hasher) {
		this.numFuncs = computeNumFuncs(numBits, numElements);
		int index = Arrays.binarySearch(sizes, numBits);
		if (index < 0) {
			// Value is not in array
			int sizePos = -index - 1;
			if (sizePos < sizes.length) {
				numBits = sizes[sizePos];
			} else {
				Logger logger = Logger.getLogger(this.getClass());
				logger.warn("Request to create Bloom Filter with " + numBits + " bits exceeds known filter sizes, using " + sizes[sizes.length - 1] + " bits");
				numBits = sizes[sizes.length - 1];
			}
		}
		// No size is a multiple of 32 so this is safe
		int numFields = (numBits / Integer.SIZE) + 1;
		filter = new int[numFields];
		this.numBits = numBits;
		this.hasher = hasher;
	}
	
	private BloomFilter(int numBits, int numFuncs, int[] filter, Hasher<T> hasher) {
		this.numBits = numBits;
		this.numFuncs = numFuncs;
		this.filter = filter;
		this.hasher = hasher;
	}

	public void add(T val) {
		final int hash1 = hasher.hashCode1(val), hash2 = hasher.hashCode2(val);
		for (int i = 0; i < numFuncs; ++i) {
			int pos = (int) ((hash1 + i * ((long) hash2)) % numBits);
			if (pos < 0) {
				pos *= -1;
			}
			int field =  pos / Integer.SIZE;
			int offset = pos % Integer.SIZE;
			int mask = 1 << offset;
			filter[field] |= mask;
		}
	}

	public boolean contains(T val) {
		final int hash1 = hasher.hashCode1(val), hash2 = hasher.hashCode2(val);
		for (int i = 0; i < numFuncs; ++i) {
			int pos = (int) ((hash1 + i * ((long) hash2)) % numBits);
			if (pos < 0) {
				pos *= -1;
			}
			int field =  pos / Integer.SIZE;
			int offset = pos % Integer.SIZE;
			int mask = 1 << offset;
			if ((filter[field] & mask) == 0) {
				return false;
			}
		}
		return true;
	}
	
	public void serialize(OutputBuffer out) {
		out.writeInt(numFuncs);
		out.writeInt(numBits);
		for (int i = 0; i < filter.length; ++i) {
			out.writeInt(filter[i]);
		}
	}
	
	public static <T> BloomFilter<T> deserialize(InputBuffer in, Hasher<T> hasher) {
		int numFuncs = in.readInt();
		int numBits = in.readInt();
		int numFields = (numBits / Integer.SIZE) + 1;
		int filter[] = new int[numFields];
		for (int i = 0; i < numFields; ++i) {
			filter[i] = in.readInt();
		}
		return new BloomFilter<T>(numBits, numFuncs, filter, hasher);
	}
}
