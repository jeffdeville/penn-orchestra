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

import java.util.Collection;
import java.util.Set;

/**
 * A set of elements that is indexed by equality and
 * a piece of derived metadata. Both <code>T</code> and
 * <code>U</code> must support contents-based equality
 * and hashing.
 * 
 * @author netaylor
 *
 * @param <T>	The type of data being stored
 * @param <U>	The type of the derived metadata
 */
public interface IndexedSet<T,U> extends Iterable<T> {
	public interface GetMetadata<T,U> {
		U getMetadata(T obj);
	}

	Set<T> getElementsForMetadata(U metadata);
	
	boolean add(T e);
	boolean addAll(Collection<? extends T> c);
	void clear();
	boolean contains(T e);
	boolean containsAll(Collection<? extends T> c);
	public boolean isEmpty();
	boolean remove(T t);
	boolean removeAll(Collection<? extends T> c);
	int size();
	
	Set<T> toSet();
	Set<U> getMetadataValues();
}
