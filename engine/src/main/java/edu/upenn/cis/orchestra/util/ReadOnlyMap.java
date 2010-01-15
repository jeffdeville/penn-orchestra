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
import java.util.Map;
import java.util.Set;

/**
 * A wrapper that takes a map and does not allow insertions or deletions to it.
 * Elements within the map can be modified if they're not immutable.
 *
 * @author netaylor
 *
 * @param <K>
 * @param <V>
 */
public class ReadOnlyMap<K,V> implements Map<K,V>, Serializable {
	private static final long serialVersionUID = 1L;
	private final Map<K,V> m;
	
	private ReadOnlyMap(Map<K,V> m) {
		if (m == null) {
			throw new NullPointerException();
		}
		this.m = m;
	}
	
	public static <K,V> ReadOnlyMap<K,V> create(Map<K,V> m) {
		if (m == null) {
			return null;
		}
		if (m instanceof ReadOnlyMap) {
			return (ReadOnlyMap<K,V>) m;
		} else {
			return new ReadOnlyMap<K,V>(m);
		}
	}
	
	public void clear() {
		throw new UnsupportedOperationException();
	}

	public boolean containsKey(Object key) {
		return m.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return m.containsValue(value);
	}

	public Set<Map.Entry<K, V>> entrySet() {
		return new ReadOnlySet<Map.Entry<K, V>>(m.entrySet());
	}

	public V get(Object key) {
		return m.get(key);
	}

	public boolean isEmpty() {
		return m.isEmpty();
	}

	public Set<K> keySet() {
		return new ReadOnlySet<K>(m.keySet());
	}

	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	public void putAll(Map<? extends K, ? extends V> t) {
		throw new UnsupportedOperationException();
	}

	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return m.size();
	}

	public Collection<V> values() {
		return new ReadOnlyCollection<V>(m.values());
	}

	public int hashCode() {
		return m.hashCode();
	}
	
	public String toString() {
		return m.toString();
	}
	
	
	public boolean equals(Object o) {
		return m.equals(o);
	}
}
