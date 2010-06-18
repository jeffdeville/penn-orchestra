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

/**
 * A LRU cache that stores a maximum number of objects
 * 
 * @author netaylor
 *
 * @param <K>		The type of the key
 * @param <V>		The type of the value
 */
public class EntryBoundLRUCache<K,V> extends LRUCache<K,V> {

	public EntryBoundLRUCache(long maxSize, EvictionHandler<? super K, ? super V> eh) {
		super(maxSize, new GetSize<V>() {
			public int getSize(V anObject) {
				return 1;
			}
		}, eh);
	}

	public EntryBoundLRUCache(long maxSize) {
		this(maxSize,null);
	}

}
