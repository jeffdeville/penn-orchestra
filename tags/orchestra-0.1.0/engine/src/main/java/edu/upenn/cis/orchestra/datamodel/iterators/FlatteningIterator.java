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

import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * An iterator that takes an iterator that returns a sequence
 * of lists and acts as if it were an iterator over the sequence
 * of lists appended together
 * 
 * @author netaylor
 *
 * @param <T>
 */
public class FlatteningIterator<T> implements ResultIterator<T> {
	private boolean subIteratorForwards = true;
	private final ResultIterator<? extends List<T>> subIterator;
	private ListIterator<T> currentIterator;
	
	public FlatteningIterator(ResultIterator<? extends List<T>> subIterator) throws IteratorException {
		this.subIterator = subIterator;
		subIteratorForwards = true;
		if (subIterator.hasNext()) {
			currentIterator = subIterator.next().listIterator();
		} else {
			currentIterator = null;
		}
	}
	
	public void close() throws IteratorException {
		subIterator.close();
	}

	public boolean hasNext() throws IteratorException {
		if (currentIterator == null) {
			return false;
		} else {
			while (! currentIterator.hasNext()) {
				if (! subIteratorForwards) {
					subIteratorForwards = true;
					// Skip over the element we're currently reading
					subIterator.next();
				}
				if (! subIterator.hasNext()) {
					return false;
				} else {
					currentIterator = subIterator.next().listIterator();
				}
			}
			return true;
		}
	}

	public boolean hasPrev() throws IteratorException {
		if (currentIterator == null) {
			return false;
		} else {
			while (! currentIterator.hasPrevious()) {
				if (subIteratorForwards) {
					subIteratorForwards = false;
					// Skip over the element we're currently reading
					subIterator.prev();
				}
				if (! subIterator.hasPrev()) {
					return false;
				} else {
					List<T> previous = subIterator.prev();
					currentIterator = previous.listIterator(previous.size());
				}
			}
			return true;
		}
	}

	public T next() throws IteratorException, NoSuchElementException {
		if (! hasNext()) {
			throw new NoSuchElementException();
		}
		return currentIterator.next();
	}

	public T prev() throws IteratorException, NoSuchElementException {
		if (! hasPrev()) {
			throw new NoSuchElementException();
		}
		return currentIterator.previous();
	}

}
