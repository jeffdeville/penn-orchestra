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
import java.util.NoSuchElementException;

public class Sequence implements Iterator<Integer> {
	private int prev;
	private final int last;
	
	public Sequence(int last) {
		this(0,last);
	}
	
	public Sequence(int start, int last) {
		prev = start - 1;
		this.last = last;
	}

	public boolean hasNext() {
		return (prev < last);
	}

	public Integer next() {
		if (prev < last) {
			++prev;
			return prev;
		} else {
			throw new NoSuchElementException();
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}
