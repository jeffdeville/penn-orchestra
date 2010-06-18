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
import java.util.Map;

public class RotatingSet<V> {
	private final Map<V,Entry<V>> findEntry;
	private Entry<V> head = null, tail = null;

	private static class Entry<V> {
		final V val;
		Entry<V> prev;
		Entry<V> next;

		Entry(V val) {
			this.val = val;
		}
	}

	public RotatingSet() {
		this(25);
	}

	public RotatingSet(int initialSize) {
		findEntry = new HashMap<V,Entry<V>>(initialSize);
	}

	public boolean add(V val) {
		if (findEntry.containsKey(val)) {
			return false;
		}
		Entry<V> e = new Entry<V>(val);
		if (head == null) {
			head = e;
			tail = e;
		} else {
			tail.next = e;
			e.prev = tail;
			tail = e;
		}
		findEntry.put(val, e);
		return true;
	}

	public boolean remove(V val) {
		Entry<V> e = findEntry.remove(val);
		if (e == null) {
			return false;
		}
		if (head == e && tail == e) {
			head = null;
			tail = null;
		} else if (head == e) {
			head = e.next;
		} else if (tail == e) {
			tail = e.prev;
		} else {
			e.next.prev = e.prev;
			e.prev.next = e.next;
		}
		return true;
	}

	public V next() {
		Entry<V> val = head;
		if (val == null) {
			return null;
		}

		if (head != tail) {
			head.next.prev = null;
			head.prev = tail;
			tail.next = head;
			tail = head;
			head = tail.next;
			tail.next = null;
		}

		return val.val;
	}
	
	public boolean isEmpty() {
		return head == null;
	}

	public void clear() {
		head = null;
		tail = null;
		findEntry.clear();
	}
}
