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

public class LongSet {
	private static class Entry {
		final long value;
		Entry next;

		Entry(long value) {
			this.value = value;
		}
	}

	private Entry[] table;
	private int size;
	private int resizeCutoff;

	private static final double maxLoad = 1.0;

	public LongSet() {
		this(10);
	}

	public LongSet(int initialSize) {
		size = 0;
		table = new Entry[initialSize];
		resizeCutoff = (int) (initialSize * maxLoad);
	}

	public LongSet(long[] ll) {
		this(ll.length);
		for (long l : ll) {
			add(l);
		}
	}

	public boolean contains(long l) {
		int hash = hash(l);
		Entry e = table[hash % table.length];
		while (e != null) {
			if (e.value == l) {
				return true;
			}
			e = e.next;
		}

		return false;
	}

	public boolean add(long l) {
		int hash = hash(l);
		int bucket = hash % table.length;
		Entry e = table[bucket];
		if (e == null) {
			table[bucket] = new Entry(l);
			++size;
			return true;
		}
		for ( ; ; ) {
			if (e.value == l) {
				return false;
			}
			if (e.next == null) {
				e.next = new Entry(l);
				++size;
				if (size > resizeCutoff) {
					resize();
				}
				return true;
			}
			e = e.next;
		}
	}

	public boolean remove(long l) {
		int hash = hash(l);
		int bucket = hash % table.length;
		Entry e = table[bucket];
		if (e == null) {
			return false;
		}
		if (e.value == l) {
			table[bucket] = table[bucket].next;
			--size;
			return true;
		}
		Entry prev = e;
		e = e.next;
		while (e != null) {
			if (e.value == l) {
				prev.next = e.next;
				--size;
				return true;
			}
			prev = e;
			e = e.next;
		}
		return false;
	}

	public void removeAll(LongSet ls) {
		for (int i = 0; i < ls.table.length; ++i) {
			Entry e = ls.table[i];
			while (e != null) {
				remove(e.value);
				e = e.next;
			}
		}
	}

	public boolean containsAll(LongSet ls) {
		for (int i = 0; i < ls.table.length; ++i) {
			Entry e = ls.table[i];
			while (e != null) {
				if (! contains(e.value)) {
					return false;
				}
				e = e.next;
			}
		}
		return true;
	}

	public boolean containsAll(long[] ls) {
		for (long l : ls) {
			if (! contains(l)) {
				return false;
			}
		}
		return true;
	}

	public boolean equals(Object o) {
		LongSet ls = (LongSet) o;
		return this.containsAll(ls) && ls.containsAll(this);
	}

	private int hash(long l) {
		int i = (int) l;
		if (i < 0) {
			return -i;
		}
		return i;
	}

	public int size() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	private void resize() {
		Entry[] oldTable = table;
		table = new Entry[table.length * 2];
		resizeCutoff = (int) (maxLoad * table.length);

		for (int i = 0; i < oldTable.length; ++i) {
			Entry e = oldTable[i];
			while (e != null) {
				Entry next = e.next;
				e.next = null;
				int hash = hash(e.value);
				int bucket = hash % table.length;

				Entry inTable = table[bucket];
				if (inTable == null) {
					table[bucket] = e; 
				} else {
					for ( ; ; ) {
						if (inTable.next == null) {
							inTable.next = e;
							break;
						}
						inTable = inTable.next;
					}
				}
				e = next;
			}
		}

	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder("{");
		boolean first = true;
		for (Entry e : table) {
			while (e != null) {
				if (! first) {
					sb.append(",");
				}
				first = false;
				sb.append(e.value);
				e = e.next;
			}
		}
		sb.append("}");
		return sb.toString();
	}
}
