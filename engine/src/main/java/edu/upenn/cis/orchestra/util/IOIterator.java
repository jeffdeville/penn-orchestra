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

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An interator-like class that operates over external data, and therefore
 * may throw IOExceptions. It can also be explicitly closed to free up
 * IO-related resources.
 * 
 * @author netaylor
 *
 * @param <T>	The type of value iterated over.
 */
public interface IOIterator<T> {
	boolean hasNext() throws IOException;
	T next() throws IOException;
	void close() throws IOException;
	
	public static class Empty<T> implements IOIterator<T> {
		@Override
		public void close() {
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public T next() {
			throw new NoSuchElementException();
		}
	};
}
