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

public class IntList {
	private int[] list;
	private int length = 0;

	public IntList() {
		list = new int[20];
	}
	
	public IntList(int initialCapacity) {
		list = new int[initialCapacity];
	}
	
	public void add(int l) {
		if (list.length == length) {
			int[] newList = new int[list.length * 2];
			System.arraycopy(list, 0, newList, 0, length);
			list = newList;
		}
		list[length++] = l;
	}

	public int[] getList() {
		int[] retval = new int[length];
		System.arraycopy(list, 0, retval, 0, length);
		return retval;
	}

	public void addFrom(IntList ii) {
		addFrom(ii, 0, ii.length, 0);
	}
	
	public void addFrom(IntList ii, int offset, int length) {
		addFrom(ii, offset, length, 0);
	}

	public void addWithShift(IntList ii, int shift) {
		addFrom(ii, 0, ii.length, shift);
	}
	
	public void addFrom(IntList ii, int offset, int length, int shift) {
		if (this.length + length > list.length) {
			int newCapacity = list.length * 2;
			while (newCapacity < (this.length + length)) {
				newCapacity *= 2;
			}
			int[] newList = new int[newCapacity];
			System.arraycopy(list, 0, newList, 0, this.length);
			list = newList;
		}
		for (int i = offset; i < offset + length; ++i) {
			list[this.length++] = ii.list[i] + shift;
		}
	}
	
	public void clear() {
		length = 0;
	}

	public int size() {
		return length;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		if (length > 0) {
			sb.append(list[0]);
		}

		for (int i =  1; i < length; ++i) {
			sb.append(',');
			sb.append(list[i]);
		}

		sb.append("]");
		return sb.toString();
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != IntList.class) {
			return false;
		}
		IntList ii = (IntList) o;
		if (ii.length != length) {
			return false;
		}
		
		for (int i = 0; i < length; ++i) {
			if (list[i] != ii.list[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	public int hashCode() {
		int retval = 0;
		for (int i = 0; i < length; ++i) {
			retval = 37 * retval + (int) list[i];
		}
		return retval;
	}
	
	public IntList addShift(int shift) {
		IntList retval = new IntList(length);
		for (int i = 0; i < length; ++i) {
			retval.list[i] = list[i] + shift;
		}
		retval.length = length;
		return retval;
	}
	
	public int get(int index) {
		return list[index];
	}
}