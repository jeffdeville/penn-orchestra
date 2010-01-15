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

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

/**
 * A wrapper that takes a collection and does not allow insertions or deletions to it.
 * Elements within the collection can be modified if they're not immutable.
 * 
 * @author netaylor
 *
 * @param <T>
 */
public class ReadOnlyCollection<T> implements Collection<T>, Serializable {
	private static final long serialVersionUID = 1L;
	private final Collection<T> c;
	
	protected ReadOnlyCollection(Collection<T> c) {
		if (c == null) {
			throw new NullPointerException();
		}
		this.c = c;
	}
	
	public static <T> ReadOnlyCollection<T> create(Collection<T> c) {
		if (c == null) {
			return null;
		}
		if (c instanceof ReadOnlyCollection) {
			return (ReadOnlyCollection<T>) c;
		} else {
			return new ReadOnlyCollection<T>(c);
		}
	}
	
	public boolean add(T arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends T> arg0) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public boolean contains(Object arg0) {
		return c.contains(arg0);
	}

	public boolean containsAll(Collection<?> arg0) {
		return c.containsAll(arg0);
	}

	public boolean isEmpty() {
		return c.isEmpty();
	}

	public Iterator<T> iterator() {
		return new Iterator<T>() {
			Iterator<T> i = c.iterator();

			public boolean hasNext() {
				return i.hasNext();
			}

			public T next() {
				return i.next();
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	public boolean remove(Object arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> arg0) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return c.size();
	}

	public Object[] toArray() {
		return c.toArray();
	}

	public <S> S[] toArray(S[] arg0) {
		return c.toArray(arg0);
	}

	public int hashCode() {
		return c.hashCode();
	}
	
	public String toString() {
		return c.toString();
	}
	
	public Object clone() {
		return this;
	}

	public boolean equals(Object o) {
		return c.equals(o);
	}
}
