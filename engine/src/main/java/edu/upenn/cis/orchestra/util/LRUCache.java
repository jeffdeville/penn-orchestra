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

import java.util.HashMap;
import java.util.ArrayList;

public class LRUCache<K,V> implements Cache<K,V> {
	public interface GetSize<T> {
		int getSize(T anObject);
	}

	private static class LinkedListElement<T> {
		LinkedListElement<T> prev;
		LinkedListElement<T> next;
		T data;
	}

	private long currSize;
	private long maxSize;
	private EvictionHandler<? super K, ? super V> eh;

	private LinkedListElement<K> head;
	private LinkedListElement<K> tail;
	private final HashMap<K,LinkedListElement<K>> listLookup;
	private final HashMap<K,V> data;
	private final GetSize<V> gs;

	public LRUCache(long maxSize, GetSize<V> gs, EvictionHandler<? super K, ? super V> eh) {
		this.maxSize = maxSize;
		this.eh = eh;
		this.currSize = 0;
		listLookup = new HashMap<K,LinkedListElement<K>>();
		data = new HashMap<K,V>();
		this.gs = gs;
	}

	public LRUCache(long maxSize, GetSize<V> gs) {
		this(maxSize, gs, null);
	}

	public synchronized void reset() {
		this.currSize = 0;
		listLookup.clear();
		data.clear();
		head = null;
		tail = null;
	}

	public synchronized void setEvictionHandler(EvictionHandler<? super K, ? super V> eh) {
		this.eh = eh;
	}

	public synchronized V probe(K key) {
		V retval = data.get(key);
		if (retval == null) {
			return null;
		}

		LinkedListElement<K> el = listLookup.get(key);
		if (el == null) {
			throw new RuntimeException("Key present in cache but not LRU list; this should not happen!");
		}
		if (head != el) {
			if (el.prev != null) {
				el.prev.next = el.next;
			}
			if (tail == el) {
				tail = el.prev;
			} else if (el.next != null) {
				el.next.prev = el.prev;
			}
			el.next = head;
			if (head != null) {
				head.prev = el;
			}
			head = el;
		}
		return retval;
	}

	public synchronized void store(K key, V value) {
		clear(key);
		if (gs.getSize(value) > maxSize) {
			if (eh != null) {
				eh.wasEvicted(key, value);
			}
			return;
		}
		ArrayList<K> evictedKey = new ArrayList<K>();
		ArrayList<V> evictedVal = new ArrayList<V>();		
		data.put(key, value);
		currSize += gs.getSize(value);
		while (currSize > maxSize) {
			K toRemove = tail.data;
			if (tail.prev != null) {
				tail.prev.next = null;
			}
			tail = tail.prev;
			listLookup.remove(toRemove);
			V removedData = data.remove(toRemove);
			currSize -= gs.getSize(removedData);
			evictedKey.add(toRemove);
			evictedVal.add(removedData);
		}
		LinkedListElement<K> el = new LinkedListElement<K>();
		el.data = key;
		if (head == null) {
			head = el;
			tail = el;
		} else {
			head.prev = el;
			el.next = head;
			head = el;
		}
		listLookup.put(key, el);
		if (eh != null) {
			for (int i = 0; i < evictedKey.size(); ++i) {
				eh.wasEvicted(evictedKey.get(i), evictedVal.get(i));
			}
		}
	}

	public synchronized void clear(K key) {
		V value = data.remove(key);
		if (value != null) {
			LinkedListElement<K> el = listLookup.get(key);
			if (el == null) {
				return;
			}
			if (head == el) {
				head = el.next;
			} else {
				el.prev.next = el.next;
			}
			if (tail == el) {
				tail = el.prev;
			} else {
				el.next.prev = el.prev;
			}
			currSize -= gs.getSize(value);
			listLookup.remove(key);
		}
	}

}
