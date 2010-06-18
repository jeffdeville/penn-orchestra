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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;


public class SubsetIterator<T> implements Iterator<Set<T>> {
	private static class BooleanArrayWrapper {
		boolean[] list;
		BooleanArrayWrapper(boolean[] list) {
			this.list = list;
		}
	
		static BooleanArrayWrapper makeComplement(boolean[] list) {
			final int listLen = list.length;
			boolean newList[] = new boolean[listLen];
			for (int i = 0; i < listLen; ++i) {
				newList[i] = (! list[i]);
			}
			return new BooleanArrayWrapper(newList);
		}
	
		public int hashCode() {
			return Arrays.hashCode(list);
		}
	
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			BooleanArrayWrapper baw = (BooleanArrayWrapper) o;
			if (list.length != baw.list.length) {
				return false;
			}
			final int len = list.length;
			for (int i = 0; i < len; ++i) {
				if (list[i] != baw.list[i]) {
					return false;
				}
			}
	
			return true;
		}
	
		public String toString() {
			return Arrays.toString(list);
		}
	}

	private final int inputSize;
	private final boolean includeComplements;
	private List<T> inputList;
	private boolean[] inSubset;
	private SubsetIterator.BooleanArrayWrapper inSubsetWrapper;
	private Set<SubsetIterator.BooleanArrayWrapper> alreadyDone = new HashSet<SubsetIterator.BooleanArrayWrapper>();
	private boolean hasNext;

	public SubsetIterator(Set<? extends T> input, boolean includeComplements) {
		inputSize = input.size();
		this.includeComplements = includeComplements;
		inputList = new ArrayList<T>(input);
		inSubset = new boolean[input.size()];
		inSubsetWrapper = new BooleanArrayWrapper(inSubset);
		hasNext = true;
		if (hasNext) {
			alreadyDone.add(BooleanArrayWrapper.makeComplement(inSubset));
		}
	}

	public boolean hasNext() {
		return hasNext;
	}

	public Set<T> next() {
		if (! hasNext) {
			throw new NoSuchElementException();
		}

		Set<T> retval;

		retval = new HashSet<T>();
		for (int i = 0; i < inputSize; ++i) {
			if (inSubset[i]) {
				retval.add(inputList.get(i));
			}
		}

		OUTER_LOOP: for ( ; ; ) {
			for (int i = 0; i <= inputSize; ++i) {
				if (i == inputSize) {
					hasNext = false;
					break OUTER_LOOP;
				}
				if (! inSubset[i]) {
					inSubset[i] = true;
					for (int j = 0; j < i; ++j) {
						inSubset[j] = false;
					}
					break;
				}
			}
			if (includeComplements) {
				break;
			}
			if (! alreadyDone.contains(inSubsetWrapper)) {
				alreadyDone.add(BooleanArrayWrapper.makeComplement(inSubset));
				break;
			}
		}
		return retval;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}