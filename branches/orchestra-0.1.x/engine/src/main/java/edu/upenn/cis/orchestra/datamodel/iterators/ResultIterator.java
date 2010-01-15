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

/**
 * General interface for a forwards and backwards iterator. It does
 * not support updates. The iterator must be closed by the method
 * that requested it in order to free underlying resources. The
 * iteration behavior should be identical to that of a ListIterator.
 * 
 * @author netaylor
 * @param <T>
 * 
 * @see java.util.ListIterator
 */
public interface ResultIterator<T> {

	/**
	 * Determine if this iterator has more elements after the current one
	 * 
	 * @return	<code>true</code> if there are following elements,
	 * 			<code>false</code> if there are not
	 * @throws IteratorException
	 */
	boolean hasNext() throws IteratorException;
	
	/**
	 * Determine if this iterator has more elements before the current one
	 * 
	 * @return	<code>true</code> if there are preceeding elements,
	 * 			<code>false</code> if there are not
	 * @throws IteratorException
	 */
	boolean hasPrev() throws IteratorException;
	
	/**
	 * Returns the next element in the iteration.
	 * 
	 * @return	The element
	 * @throws IteratorException
	 * @throws NoSuchElementException if the iteration has no more elements
	 */
	T next() throws IteratorException, NoSuchElementException;
	
	/**
	 * Returns the previous element in the iteration.
	 * 
	 * @return	The element
	 * @throws IteratorException
	 * @throws NoSuchElementException if the iteration has no preceding elements
	 */
	T prev() throws IteratorException, NoSuchElementException;

	/**
	 * Closes the iterator. This must be called to free resources,
	 * such as a JDBC <code>ResultSet</code>, or a BerkeleyDB <code>Cursor</code>. 
	 * @throws IteratorException
	 */
	void close() throws IteratorException;
}
