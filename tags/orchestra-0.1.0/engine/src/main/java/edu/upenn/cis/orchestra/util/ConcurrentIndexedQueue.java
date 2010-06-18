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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implements an indexed queue. Supports normal queue operations
 * in addition to efficient removal by value. Contains is implemented
 * in a lock-free manner.
 * 
 * 
 * @author netaylor
 *
 * @param <E>
 * @param <M>
 */
public class ConcurrentIndexedQueue<E,M> implements Queue<E> {
	private static class Element<E,M> {
		final E data;
		final M metadata;
		Element<E,M> next;
		Element<E,M> prev;
		
		Element(E data, M metadata) {
			this.data = data;
			this.metadata = metadata;
		}
	}
	
	public interface MetadataFactory<E,M> {
		M getMetadata(E element);
	};
		
	private final MetadataFactory<E,M> metadataFactory;
	
	
	private final ConcurrentMap<E,Element<E,M>> index = new ConcurrentHashMap<E,Element<E,M>>();

	private Element<E,M> head = null, tail = null;
	
	private M skip = null;
	
	private Element<E,M> skipPointer = null;
	
	public ConcurrentIndexedQueue() {
		this.metadataFactory = new MetadataFactory<E,M>() {

			public M getMetadata(E element) {
				return null;
			}
			
		};
	}
	
	public ConcurrentIndexedQueue(MetadataFactory<E,M> metadataFactory) {
		this.metadataFactory = metadataFactory;
	}
	
	public synchronized void setSkipMetadata(M metadata) {
		if (metadataFactory == null) {
			throw new IllegalArgumentException("Cannot set skip metadata without a metadata factory");
		}
		if (metadata == null) {
			throw new IllegalArgumentException("Cannot supply null metadata");
		}
		skip = metadata;
		skipPointer = null;
	}
	
	public synchronized void clearSkipMetadata() {
		if (metadataFactory == null) {
			throw new IllegalArgumentException("Cannot clear skip metadata without a metadata factory");
		}
		skip = null;
		skipPointer = null;
	}
	
	public synchronized boolean add(E e) {
		if (index.containsKey(e)) {
			return true;
		}
		
		Element<E,M> el = new Element<E,M>(e, metadataFactory.getMetadata(e));
		if (head == null) {
			head = el;
			tail = el;
		} else {
			tail.next = el;
			el.prev = tail;
			tail = el;
		}
		
		index.put(e, el);
		
		return true;
	}

	public synchronized boolean offer(E e) {
		return add(e);
	}

	public synchronized E element() {
		throw new UnsupportedOperationException();
	}

	public synchronized E peek() {
		throw new UnsupportedOperationException();
	}

	public synchronized E poll() {
		if (head == null) {
			return null;
		}
		E retval;
		if (skip == null) {
			retval = head.data;
			head = head.next;
			if (head == null) {
				tail = null;
			}
		} else {
			boolean considerCurrent = false;
			if (skipPointer == null) {
				skipPointer = head;
				considerCurrent = true;
			}
			while (skipPointer.metadata.equals(skip) || (! considerCurrent)) {
				if (skipPointer.next == null) {
					break;
				} else {
					skipPointer = skipPointer.next;
					considerCurrent = true;
				}
			}
			if (skipPointer.metadata.equals(skip)) {
				retval = null;
			} else if (considerCurrent) {
				retval = skipPointer.data;
				if (skipPointer.prev != null) {
					skipPointer.prev.next = skipPointer.next;
				}
				if (skipPointer.next != null) {
					skipPointer.next.prev = skipPointer.prev;
				}
				if (head == skipPointer) {
					head = skipPointer.next;
				}
			} else {
				retval = null;
			}
		}
		if (retval != null) {
			if (index.remove(retval) == null) {
				throw new IllegalStateException("Didn't find value to remove for " + retval);
			}
		}
		return retval;
	}

	public synchronized E remove() {
		E retval = poll();
		if (retval == null) {
			throw new NoSuchElementException();
		}
		return retval;
	}

	public synchronized boolean addAll(Collection<? extends E> c) {
		for (E e : c) {
			add(e);
		}
		return true;
	}

	public synchronized void clear() {
		index.clear();
		head = null;
		tail = null;
	}

	public boolean contains(Object o) {
		return index.containsKey(o);
	}

	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (! index.containsKey(o)) {
				return false;
			}
		}
		
		return true;
	}

	public synchronized boolean isEmpty() {
		return head == null;
	}

	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	public synchronized boolean remove(Object o) {
		Element<E,M> el = index.remove(o);
		if (el == null) {
			return false;
		}
		
		if (head == el && tail == el) {
			head = null;
			tail = null;
		} else if (head == el) {
			head.next.prev = null;
			head = head.next;
		} else if (tail == el) {
			tail.prev.next = null;
			tail = tail.prev;
		} else {
			if (el.next == null || el.prev == null) {
				throw new IllegalStateException("Should not have null link if not tail or head");
			}
			el.next.prev = el.prev;
			el.prev.next = el.next;
		}
				
		return true;
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return index.size();
	}

	public synchronized Object[] toArray() {
		Object[] retval = new Object[index.size()];
		int pos = 0;
		for (Element<E,M> el = head; el != null; el = el.next) {
			retval[pos++] = el.data;
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> T[] toArray(T[] a) {
		Object[] array = toArray();
		if (a.length >= array.length) {
			System.arraycopy(array, 0, a, 0, array.length);
			if (a.length > array.length) {
				a[array.length] = null;
			}
			return a;
		} else {
			return (T[]) Arrays.copyOf(array, array.length, a.getClass());
		}
	}

}
