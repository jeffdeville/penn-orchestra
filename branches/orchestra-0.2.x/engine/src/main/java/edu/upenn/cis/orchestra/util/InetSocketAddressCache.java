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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.upenn.cis.orchestra.datamodel.IntType;

public class InetSocketAddressCache {
	private final Map<ByteArrayWrapper,InetSocketAddress> cache = new HashMap<ByteArrayWrapper,InetSocketAddress>();
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();
	
	public InetSocketAddress probe(byte[] serializedForm) throws UnknownHostException {
		ByteArrayWrapper baw = new ByteArrayWrapper(serializedForm);
		readLock.lock();
		try {
			InetSocketAddress cached = cache.get(baw);
			if (cached != null) {
				return cached;
			}
		} finally {
			readLock.unlock();
		}
		
		byte[] addr = new byte[OutputBuffer.addrLen];
		for (int i = 0; i < OutputBuffer.addrLen; ++i) {
			addr[i] = serializedForm[i];
		}
		InetAddress ia = InetAddress.getByAddress(addr);
		int port = IntType.getShortValFromBytes(serializedForm, OutputBuffer.addrLen, OutputBuffer.shortLen);
		InetSocketAddress deserialized = new InetSocketAddress(ia, port);
		writeLock.lock();
		try {
			cache.put(baw, deserialized);
		} finally {
			writeLock.unlock();
		}
		return deserialized;	
	}
}
