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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IndexedHashSet<T,U> implements IndexedSet<T, U> {	
	private final GetMetadata<? super T, ? extends U> gm;
	// The index should never contain an empty set
	private final HashMap<U,Set<T>> index = new HashMap<U,Set<T>>();
	private final Set<T> empty = Collections.emptySet();
	
	public IndexedHashSet(GetMetadata<? super T, ? extends U> gm) {
		this.gm = gm;
	}
	
	
	public Set<T> getElementsForMetadata(U metadata) {
		Set<T> els = index.get(metadata);
		if (els != null) {
			return Collections.unmodifiableSet(els);
		} else {
			return empty;
		}
	}

	public boolean add(T e) {
		U md = gm.getMetadata(e);
		Set<T> set = index.get(md);
		if (set == null) {
			set = new HashSet<T>();
			index.put(md, set);
		}
		return set.add(e);
	}
	

	public boolean addAll(Collection<? extends T> c) {
		boolean changed = false;
		for (T t : c) {
			if (add(t)) {
				changed = true;
			}
		}
		
		return changed;
	}

	public void clear() {
		index.clear();
	}
	
	
	public boolean contains(T e) {
		return getElementsForMetadata(gm.getMetadata(e)).contains(e);
	}
	
	
	public boolean containsAll(Collection<? extends T> c) {
		for (T t : c) {
			if (! contains(t)) {
				return false;
			}
		}
		
		return true;
	}
	
	
	public boolean isEmpty() {
		return index.isEmpty();
	}
	
	
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			Iterator<Set<T>> setIt = index.values().iterator();
			Set<T> currSet;
			Iterator<T> currIt = empty.iterator();
			
			
			public boolean hasNext() {
				return currIt.hasNext() || setIt.hasNext();
			}

			
			public T next() {
				if (! currIt.hasNext()) {
					currSet = setIt.next();
					currIt = currSet.iterator();
				}
				return currIt.next();
			}

			
			public void remove() {
				currIt.remove();
				if (currSet.isEmpty()) {
					setIt.remove();
				}
			}
			
		};
	}
	
	
	public boolean remove(T t) {
		U md = gm.getMetadata(t);
		Set<T> setForMD = index.get(md);
		if (setForMD == null) {
			return false;
		}
		boolean retval = setForMD.remove(t);
		if (setForMD.isEmpty()) {
			index.remove(md);
		}
		return retval;
	}
	
	public boolean removeAll(Collection<? extends T> c) {
		boolean retval = false;
		for (T t : c) {
			if (remove(t)) {
				retval = true;
			}
		}
		return retval;
	}


	
	public int size() {
		int size = 0;
		for (Set<T> set : index.values()) {
			size += set.size();
		}
		return size;
	}
	
	public Set<U> getMetadataValues() {
		return Collections.unmodifiableSet(index.keySet());
	}

	
	public Set<T> toSet() {
		Set<T> retval = new HashSet<T>();
		for (Set<T> set : index.values()) {
			retval.addAll(set);
		}
		return retval;
	}
	
	public String toString() {
		return toSet().toString();
	}
	
	@SuppressWarnings("unchecked")
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		IndexedHashSet ihs = (IndexedHashSet) o;
		return ihs.toSet().equals(toSet());
	}
}
