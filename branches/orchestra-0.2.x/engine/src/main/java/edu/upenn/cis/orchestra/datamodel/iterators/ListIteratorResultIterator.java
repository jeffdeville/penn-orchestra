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

import java.util.ListIterator;
import java.util.NoSuchElementException;


/**
 * A wrapper to convert a ListIterator into a ResultIterator. Since
 * the ResultIterator interface does not support updates, the underlying
 * list cannot be changed through this class (unless the list elements
 * themselves are mutable).
 * 
 * @author netaylor
 *
 * @param <T>
 */
public class ListIteratorResultIterator<T> implements ResultIterator<T> {
	ListIterator<T> li;
	
	public ListIteratorResultIterator(ListIterator<T> source) {
		li = source;
	}
	
	public void close() {
		li = null;
	}

	public boolean hasNext(){
		return li.hasNext();
	}
	

	public boolean hasPrev(){
		return li.hasPrevious();
	}

	public T next() throws NoSuchElementException {
		return li.next();
	}

	public T prev() throws NoSuchElementException {
		return li.previous();
	}

}
