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

import java.util.Iterator;
import java.util.NoSuchElementException;

import edu.upenn.cis.orchestra.logicaltypes.PrimeNumbers;

public class ByteArraySet {
	private final byte[][] arrays;
	private final int[] offsets;
	private final int[] lengths;
	private final Overflow[] overflows;
	private int size;
	
	private static class Overflow {
		final byte[] array;
		final int offset;
		final int length;
		Overflow next;
		
		Overflow(byte[] array, int offset, int length) {
			this.array = array;
			this.offset = offset;
			this.length = length;
		}
	}
	
	public ByteArraySet(int expectedSize) {
		this(expectedSize,0.5);
	}
	
	public ByteArraySet(int expectedSize, double desiredOccupancy) {
		final int numBuckets = PrimeNumbers.getNextLargerPrime((int) (expectedSize / desiredOccupancy));
		arrays = new byte[numBuckets][];
		offsets = new int[numBuckets];
		lengths = new int[numBuckets];
		overflows = new Overflow[numBuckets];
		size = 0;
	}
	
	public boolean add(byte[] array) {
		return add(array, 0, array.length);
	}
	
	private static boolean equals(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
		if (l1 != l2) {
			return false;
		}
		for (int i = 0; i < l1; ++i) {
			if (b1[o1+i] != b2[o2+i]) {
				return false;
			}
		}
		return true;
	}
	
	public boolean add(byte[] array, int offset, int length) {
		final int hashCode = ByteArrayWrapper.hashCode(array, offset, length);
		int bucket = hashCode % arrays.length;
		if (bucket < 0) {
			bucket += arrays.length;
		}
		if (arrays[bucket] == null) {
			arrays[bucket] = array;
			offsets[bucket] = offset;
			lengths[bucket] = length;
			++size;
			return true;
		} else if (equals(array, offset, length, arrays[bucket], offsets[bucket], lengths[bucket])) {
			return false;
		}
		if (overflows[bucket] == null) {
			// Add to overflow list
			overflows[bucket] = new Overflow(array, offset, length);
			++size;
			return true;
		}
		
		// Add to overflow list, if not present
		Overflow o = overflows[bucket];
		while (o.next != null) {
			if (equals(array,offset,length, o.array, o.offset, o.length)) {
				return false;
			}
			o = o.next;
		}
		if (equals(array, offset, length, o.array, o.offset, o.length)) {
			return false;
		} else {
			o.next = new Overflow(array, offset, length);
			++size;
			return true;
		}
	}
	
	public boolean remove(byte[] array) {
		return remove(array, 0, array.length);
	}
	
	public boolean remove(byte[] array, int offset, int length) {
		final int hashCode = ByteArrayWrapper.hashCode(array, offset, length);
		int bucket = hashCode % arrays.length;
		if (bucket < 0) {
			bucket += arrays.length;
		}
		if (arrays[bucket] == null) {
			return false;
		} else if (equals(array, offset, length, arrays[bucket], offsets[bucket], lengths[bucket])) {
			Overflow o = overflows[bucket];
			if (o == null) {
				arrays[bucket] = null;
			} else {
				overflows[bucket] = o.next;
				arrays[bucket] = o.array;
				offsets[bucket] = o.offset;
				lengths[bucket] = o.length;
			}
			--size;
			return true;
		}
		// Find and remove from overflow list
		Overflow o = overflows[bucket];
		Overflow prev = null;
		while (o != null) {
			if (equals(array, offset, length, o.array, o.offset, o.length)) {
				if (prev == null) {
					overflows[bucket] = o.next;
				} else {
					prev.next = o.next;
				}
				--size;
				return true;
			}
			prev = o;
			o = o.next;
		}
		return false;
	}
	
	public boolean contains(byte[] array) {
		return contains(array, 0, array.length);
	}
	
	public boolean contains(byte[] array, int offset, int length) {
		final int hashCode = ByteArrayWrapper.hashCode(array, offset, length);
		int bucket = hashCode % arrays.length;
		if (bucket < 0) {
			bucket += arrays.length;
		}
		if (arrays[bucket] == null) {
			return false;
		} else if (equals(array, offset, length, arrays[bucket], offsets[bucket], lengths[bucket])) {
			return true;
		}
		// Find and remove from overflow list
		Overflow o = overflows[bucket];
		while (o != null) {
			if (equals(array, offset, length, o.array, o.offset, o.length)) {
				return true;
			}
			o = o.next;
		}
		return false;
	}
	
	public interface Deserializer<T> {
		T fromBytes(byte[] data, int offset, int length);
	}
	
	public <T> Iterator<T> iterator(final Deserializer<T> deserializer) {
		return new Iterator<T>() {
			T next;

			private int pos = -1;
			private Overflow o = null;
			
			{
				next = advance();
			}
			
			private T advance() {
				if (o == null) {
					++pos;
					while (pos < arrays.length && arrays[pos] == null) {
						++pos;
					}
					if (pos >= arrays.length) {
						return null;
					}
					o = overflows[pos];
					return deserializer.fromBytes(arrays[pos], offsets[pos], lengths[pos]);
				} else {
					T retval = deserializer.fromBytes(o.array, o.offset, o.length);
					o = o.next;
					return retval;
				}
			}
			
			
			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public T next() {
				if (next == null) {
					throw new NoSuchElementException();
				}
				T retval = next;
				next = advance();
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
	
	public int size() {
		return size;
	}
	
	public void clear() {
		for (int i = 0; i < arrays.length; ++i) {
			arrays[i] = null;
			overflows[i] = null;
		}
		size = 0;
	}
	
	public int numBuckets() {
		return arrays.length;
	}
	
	public boolean isEmpty() {
		return size() == 0;
	}
}
