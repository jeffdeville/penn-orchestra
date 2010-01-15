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


public abstract class MappingIterator<T,U> implements ResultIterator<T> {
	final ResultIterator<? extends U> subIterator;
	
	public MappingIterator(ResultIterator<? extends U> subIterator) {
		this.subIterator = subIterator;
	}

	public void close() throws IteratorException {
		subIterator.close();
	}

	public boolean hasNext() throws IteratorException {
		return subIterator.hasNext();
	}

	public boolean hasPrev() throws IteratorException {
		return subIterator.hasPrev();
	}

	public T next() throws IteratorException, NoSuchElementException {
		return convert(subIterator.next());
	}

	public T prev() throws IteratorException, NoSuchElementException {
		return convert(subIterator.prev());
	}
	
	protected abstract T convert(U input) throws IteratorException;
}