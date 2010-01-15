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
package edu.upenn.cis.orchestra.datamodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Collection;



public abstract class AbstractTupleSet<S extends AbstractRelation, T extends AbstractTuple<S>> implements Set<T>, Serializable {
	private static final long serialVersionUID = 1L;
	private final HashMap<KeyTuple,T> map;
	private final Class<S> sClass;
	
	protected abstract Class<S> getSchemaClass();
	
	public AbstractTupleSet() {
		map = new HashMap<KeyTuple,T>();
		this.sClass = getSchemaClass();
	}

	public AbstractTupleSet(int initialSize) {
		map = new HashMap<KeyTuple,T>(initialSize);
		this.sClass = getSchemaClass();
	}

	public AbstractTupleSet(Collection<? extends T> tuples) {
		this(tuples.size());
		addAll(tuples);
	}

	private class KeyTuple {
		final AbstractTuple<S> tuple;

		KeyTuple(AbstractTuple<S> tuple) {
			this.tuple = tuple;
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			KeyTuple kt = (KeyTuple) o;

			
			return tuple.sameSchemaAs(kt.tuple) && tuple.sameKey(kt.tuple);
		}

		public int hashCode() {
			return tuple.keyHashCode();
		}
	}

	public Iterator<T> iterator() {
		return map.values().iterator();
	}

	public boolean add(T t) {
		if (! t.isReadOnly()) {
			throw new IllegalArgumentException("Cannot add mutable tuple to set");
		}
		KeyTuple kt = new KeyTuple(t);
		T already = map.get(kt);
		if (already == null || (! already.equals(t))) {
			map.put(kt, t);
			return true;
		} else {
			return false;
		}
	}

	public boolean addAll(Collection<? extends T> c) {
		boolean retval = false;
		for (T t : c) {
			if (add(t)) {
				retval = true;
			}
		}
		return retval;
	}

	public void clear() {
		map.clear();
	}

	public boolean containsKey(AbstractTuple<S> t) {
		KeyTuple kt = new KeyTuple(t);
		return map.containsKey(kt);
	}

	@SuppressWarnings("unchecked")
	public boolean contains(Object o) {
		AbstractTuple<?> t = (AbstractTuple<?>) o;
		if (! sClass.isInstance(t.schema)) {
			return false;
		}
		KeyTuple kt = new KeyTuple((AbstractTuple<S>) t);
		AbstractTuple<?> inMap = map.get(kt);
		if (inMap == null || (! inMap.equals(t))) {
			return false;
		} else {
			return true;
		}
	}

	public boolean containsAllKeys(Collection<? extends AbstractTuple<S>> c) {
		for (AbstractTuple<S> t : c) {
			if (! containsKey(t)) {
				return false;
			}
		}
		return true;
	}

	public boolean containsAll(Collection<?> c) {
		boolean retval = true;
		for (Object o : c) {
			if (! contains(o)) {
				retval = false;
				break;
			}
		}
		return retval;
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public boolean remove(Object o) {
		AbstractTuple<?> t = (AbstractTuple<?>) o;
		if (! sClass.isInstance(t.schema)) {
			return false;
		}
		KeyTuple kt = new KeyTuple((AbstractTuple<S>)t);

		// Remove any tuple with the same key as the supplied one
		AbstractTuple<?> tt = map.remove(kt);
		return (tt != null);

	}

	public boolean removeAll(Collection<?> c) {
		boolean retval = true;
		for (Object o : c) {
			if (! remove(o)) {
				retval = false;
			}
		}

		return retval;
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("I don't think anyone needs this");
	}

	public int size() {
		return map.size();
	}

	public Object[] toArray() {
		return map.values().toArray();
	}

	public <U> U[] toArray(U[] a) {
		return map.values().toArray(a);
	}

	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}

		try {
			Set<?> s = (Set<?>) o;
			for (Object tuple : s) {
				if (! contains(tuple)) {
					return false;
				}
			}
			for (AbstractTuple<?> tuple : this) {
				if (! s.contains(tuple)) {
					return false;
				}
			}
		} catch (ClassCastException cce) {
			return false;
		}
		return true;
	}

	public int hashCode() {
		return map.hashCode();
	}

	/**
	 * Get the tuple with the same key as the supplied tuple, if there is one
	 * 
	 * @param key		A tuple with the desired key
	 * @return			The tuple, or <code>null</code> if there is not
	 * 					such a tuple in this TupleSet
	 */
	public T get(T key) {
		KeyTuple kt = new KeyTuple(key);
		T t = map.get(kt);
		return t;
	}

	public String toString() {
		return map.values().toString();
	}
}
