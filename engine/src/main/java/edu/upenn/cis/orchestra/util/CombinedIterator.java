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

import java.util.Iterator;
import java.util.List;

public class CombinedIterator<T> implements Iterator<T> {
	private final Iterator<? extends Iterator<? extends T>> its;
	private Iterator<? extends T> it;
	
	public CombinedIterator(List<? extends Iterator<? extends T>> iterators) {
		its = iterators.iterator();
		if (its.hasNext()) {
			it = its.next();
			advance();
		} else {
			it = null;
		}
	}
	
	public boolean hasNext() {
		return (it != null);
	}

	public T next() {
		T next = it.next();
		advance();
		return next;
	}

	private void advance() {
		while (! it.hasNext()) {
			if (its.hasNext()) {
				it = its.next();
			} else {
				it = null;
				break;
			}
		}
	}
	
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
