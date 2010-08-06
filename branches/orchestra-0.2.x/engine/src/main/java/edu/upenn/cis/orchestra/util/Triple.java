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

public class Triple<T, U, V> {
	private final T first;
	private final U second;
	private final V third;

	public Triple(T first, U second, V third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public T getFirst() {
		return first;
	}

	public U getSecond() {
		return second;
	}
	
	public V getThird() {
		return third;
	}
		
	public int hashCode() {
		return (first == null ? 0 : first.hashCode()) + 37 * (second == null ? 0 : second.hashCode()) + 
			1601 * (third == null ? 0 : third.hashCode());
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Triple<?,?,?> p = (Triple<?,?,?>) o;
		if (first == null && p.first != null) {
			return false;
		} else if (first != null && (! first.equals(p.first))) {
			return false;
		}
		if (second == null && p.second != null) {
			return false;
		} else if (second != null && (! second.equals(p.second))) {
			return false;
		}
		if (third == null) {
			return (p.third == null);
		} else {
			return third.equals(p.third);
		}
	}
	
	public String toString() {
		return "(" + first + "," + second + "," + third + ")";
	}
}
