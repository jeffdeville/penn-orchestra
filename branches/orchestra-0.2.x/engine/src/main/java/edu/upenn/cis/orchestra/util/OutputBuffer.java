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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.datamodel.IntType;


public abstract class OutputBuffer {
	public static final int addrLen = 4; // Assume that all addresses are IP4
	public static final int shortLen = 2; // Short gets 2 bytes
	public static final int inetSocketAddressLen = addrLen + shortLen;

	public final void writeBytes(byte[] b) {
		if (b == null) {
			writeInt(-1);
		} else {
			writeInt(b.length);
			writeBytesNoLength(b);
		}
	}
	
	public final void writeBytes(byte[] b, int offset, int length) {
		if (b == null) {
			writeInt(-1);
		} else {
			writeInt(length);
			writeBytesNoLength(b, offset, length);
		}
	}

	public abstract void writeInt(int v);

	public abstract void writeShort(short s);

	public abstract void writeLong(long l);

	public abstract void writeBytesNoLength(byte[] bytes);
	
	public abstract void writeBytesNoLength(byte[] bytes, int offset, int length);

	public void writeString(String str) {
		if (str == null) {
			writeBytes(null);
		} else {
			try {
				writeBytes(str.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void writeObject(Object o) {
		if (o == null) {
			writeBytes(null);
			return;
		}
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(o);
			oos.close();
			writeBytes(baos.toByteArray());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public abstract void writeBoolean(boolean bool);

	public final void writeInetAddress(InetAddress addr) {
		byte[] addrBytes = addr.getAddress();
		if (addrBytes.length != addrLen) {
			throw new IllegalArgumentException("All addresses should be " + addrLen + " bytes long");
		}
		writeBytesNoLength(addrBytes);
	}

	public final void writeInetSocketAddress(InetSocketAddress isa) {
		writeInetAddress(isa.getAddress());
		// Turn into signed short
		writeShort((short) isa.getPort());
	}
	
	public static void getBytes(InetSocketAddress isa, byte[] dest, int offset) {
		byte[] addrBytes = isa.getAddress().getAddress();
		if (addrBytes.length != addrLen) {
			throw new IllegalArgumentException("All addresses should be " + addrLen + " bytes long");
		}
		for (int i = 0; i < addrLen; ++i) {
			dest[offset + i] = addrBytes[i];
		}
		IntType.getShortValBytes(isa.getPort(), dest, offset + addrLen);
	}
	
	public static byte[] getBytes(InetSocketAddress isa) {
		byte[] retval = new byte[inetSocketAddressLen];
		getBytes(isa, retval, 0);
		return retval;
	}
}