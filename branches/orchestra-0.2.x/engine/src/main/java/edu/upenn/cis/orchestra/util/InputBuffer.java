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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import edu.upenn.cis.orchestra.datamodel.IntType;



public abstract class InputBuffer {
	public InputBuffer() {
		cache = null;
	}

	public InputBuffer(InetSocketAddressCache cache) {
		this.cache = cache;
	}

	private final InetSocketAddressCache cache;
	public abstract byte[] readBytes(int length);

	public final byte[] readBytes() {
		int length = readInt();
		if (length < 0) {
			return null;
		} else {
			return readBytes(length);
		}
	}

	public int lastReadOffset;
	public int lastReadLength;

	public final byte[] readBytesWithoutCopying() {
		lastReadLength = readInt();
		if (lastReadLength < 0) {
			return null;
		} else {
			return readBytesWithoutCopying(lastReadLength);
		}
	}

	public abstract byte[] readBytesWithoutCopying(int length);

	public final ByteArrayWrapper readByteArrayWrapperWithoutCopying() {
		int length = readInt();
		if (length < 0) {
			return null;
		} else {
			return readByteArrayWrapperWithoutCopying(length);
		}
	}

	public abstract ByteArrayWrapper readByteArrayWrapperWithoutCopying(int length);

	public abstract int readInt();

	public abstract short readShort();

	public abstract long readLong();

	public String readString() {
		byte[] bytes = readBytes();
		if (bytes == null) {
			return null;
		} else {
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public final Object readObject() throws IOException, ClassNotFoundException {
		byte[] data = readBytes();
		if (data == null) {
			return null;
		}
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
		return ois.readObject();
	}

	public abstract boolean readBoolean();

	public final InetAddress readInetAddress() {
		try {
			byte[] addr = readBytes(OutputBuffer.addrLen);
			return InetAddress.getByAddress(addr);
		} catch (UnknownHostException e) {
			throw new RuntimeException("Shouldn't happen", e);
		}
	}

	public final InetSocketAddress readInetSocketAddress() {
		if (cache == null) {
			InetAddress addr = readInetAddress();
			// Turn back into an int
			int port = (readShort() & 0xFFFF);
			return new InetSocketAddress(addr,port);
		} else {
			byte[] addr = readBytes(OutputBuffer.inetSocketAddressLen);
			try {
				return cache.probe(addr);
			} catch (UnknownHostException e) {
				throw new RuntimeException("Shouldn't happen", e);
			}
		}
	}

	public static InetSocketAddress getInetSocketAddress(byte[] data, int offset) {
		byte[] address = new byte[OutputBuffer.addrLen];
		for (int i = 0; i < address.length; ++i) {
			address[i] = data[offset + i];
		}
		InetAddress ia;
		try {
			ia = InetAddress.getByAddress(address);
		} catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
		int port = IntType.getShortValFromBytes(data, offset + address.length, IntType.bytesPerShortInt);
		return new InetSocketAddress(ia, port);
	}
}