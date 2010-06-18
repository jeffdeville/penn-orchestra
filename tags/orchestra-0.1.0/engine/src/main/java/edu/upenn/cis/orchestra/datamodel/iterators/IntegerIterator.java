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
package edu.upenn.cis.orchestra.datamodel.iterators;

import java.util.NoSuchElementException;


public abstract class IntegerIterator<T> implements ResultIterator<T> {
	private final int last;
	private final int first;
	private int current = -1;
	private boolean forwards = true;
	
	public IntegerIterator(int last) {
		this(0,last);
	}
	
	public IntegerIterator(int first, int last) {
		this.current = first - 1;
		this.last = last;
		this.first = first;
	}
	
	public void close() {
	}

	public boolean hasNext() {
		if (forwards) {
			return (current < last);
		} else {
			return (current <= last);
		}
	}

	public boolean hasPrev() {
		if (forwards) {
			return (current >= first);
		} else {
			return (current > first);
		}
	}

	public T next() throws IteratorException, NoSuchElementException {
		if (! hasNext()) {
			throw new NoSuchElementException();
		}
		if (forwards) {
			++current;
		} else {
			forwards = true;
		}
		return getData(current);
	}

	public T prev() throws IteratorException, NoSuchElementException {
		if (! hasPrev()) {
			throw new NoSuchElementException();
		}
		if (forwards) {
			forwards = false;
		} else {
			--current;
		}
		return getData(current);
	}
	
	protected abstract T getData(int pos) throws IteratorException;
}