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
import java.util.HashSet;
import java.util.Set;

/**
 * A wrapper that takes a set and does not allow insertions or deletions to it.
 * Elements within the set can be modified if they're not immutable.
 * 
 * @author Nick
 *
 * @param <E>
 */
public class ReadOnlySet<E> extends ReadOnlyCollection<E> implements Set<E> {
	private static final long serialVersionUID = 1L;

	ReadOnlySet(Set<E> s) {
		super(s);
	}
	
	public static <E> ReadOnlySet<E> create(Collection<E> s) {
		if (s == null) {
			return null;
		}
		if (s instanceof ReadOnlySet) {
			return (ReadOnlySet<E>) s;
		} else if (s instanceof Set){
			return new ReadOnlySet<E>((Set<E>) s);
		} else {
			return new ReadOnlySet<E>(new HashSet<E>(s));
		}
	}
}
