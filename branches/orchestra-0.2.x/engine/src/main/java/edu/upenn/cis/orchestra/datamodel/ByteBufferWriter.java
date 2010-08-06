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
package edu.upenn.cis.orchestra.datamodel;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;


public class ByteBufferWriter {
	public static final int DEFAULT_INITIAL_ALLOCATION = 10;
	public static final int MAX_SIZE_TO_SAVE = 1000;

	public ByteBufferWriter() {
		this(null,DEFAULT_INITIAL_ALLOCATION);
	}

	public ByteBufferWriter(int initialAllocation) {
		this(null,initialAllocation);
	}

	public ByteBufferWriter(byte[] initialContents) {
		this(initialContents,0);
	}

	public ByteBufferWriter(byte[] initialContents, int initialAllocation) {
		if (initialContents != null) {
			if (initialAllocation > initialContents.length) {
				buffer = new byte[initialAllocation];
			} else {
				buffer = new byte[initialContents.length];
			}
			System.arraycopy(initialContents, 0, buffer, 0, initialContents.length);
			dataLength = initialContents.length;
		} else {
			buffer = new byte[initialAllocation];
		}
	}

	private byte[] buffer;
	private int dataLength = 0;

	/**
	 * Write the length of this byte array, followed by the byte array itself.
	 * 
	 * @param bytes		The byte array to write
	 */
	public void addToBuffer(byte[] bytes) {
		if (bytes == null) {
			addToBuffer(bytes, -1, -1);
		} else {
			addToBuffer(bytes, 0, bytes.length);
		}
	}

	public void addToBuffer(byte[] bytes, int offset, int length) {
		if (bytes == null) {
			addToBuffer(-1);
		} else {
			addToBuffer(length);
			addToBufferNoLength(bytes, offset, length);
		}
	}


	public void addToBuffer(ByteArrayWrapper b) {
		addToBuffer(b.array, b.offset, b.length);
	}

	/**
	 * Add an integer value to the byte array
	 * 
	 * @param val		The value to add
	 */
	public void addToBuffer(int val) {
		this.ensureCapacity(dataLength + IntType.bytesPerInt);
		IntType.putBytes(val, buffer, dataLength);
		dataLength += IntType.bytesPerInt;
	}

	/**
	 * Add a Tuple to the byte array (preceeded by its length)
	 * 
	 * @param t			The value to add
	 */
	public void addToBuffer(Tuple t) {
		addToBuffer(t == null ? null : t.getBytes());
	}

	public void addToBuffer(String s) {
		try {
			addToBuffer(s == null ? null : s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Could not convert String to UTF-8",e);
		}
	}

	/**
	 * Add a transaction/peer id to the byte array (preceeded by its length)
	 * 
	 * @param tpi
	 */
	public void addToBuffer(TxnPeerID tpi) {
		addToBuffer(tpi == null ? null : tpi.getBytes());
	}

	/**
	 * Add an update to this byte array
	 * 
	 * @param u				The update to add
	 * @param sl			The serialization level at which to add the update
	 */
	public void addToBuffer(Update u, Update.SerializationLevel sl) {
		addToBuffer(u == null ? null : u.getBytes(sl));
	}

	/**
	 * Add a byte value to the byte array
	 * 
	 * @param b
	 */
	public void addToBuffer(byte b) {
		ensureCapacity(dataLength + 1);
		buffer[dataLength++] = b;
	}

	public void addToBufferNoLength(byte[] data) {
		addToBufferNoLength(data, 0, data.length);
	}

	private void ensureCapacity(int capacity) {
		if (capacity > buffer.length) {
			int newLength = buffer.length * 2;
			if (newLength < capacity) {
				newLength = capacity;
			}
			byte newBuffer[] = new byte[newLength];
			System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
			buffer = newBuffer;
		}
	}

	public void addToBufferNoLength(byte[] data, int offset, int length) {
		ensureCapacity(dataLength + length);
		System.arraycopy(data, offset, buffer, dataLength, length);
		dataLength += length;

	}

	/**
	 * Add a PeerID to the byte array
	 * 
	 * @param pid
	 */
	public void addToBuffer(AbstractPeerID pid) {
		addToBuffer(pid == null ? null : pid.getBytes());
	}

	/**
	 * Add a boolean value to the byte array
	 * 
	 * @param b		The boolean to add
	 */
	public void addToBuffer(boolean b) {
		addToBuffer((byte) (b ? 1 : 0));
	}

	public void addToBuffer(PidAndRecno par) {
		addToBuffer(par.getPid());
		addToBuffer(par.getRecno());
	}

	/**
	 * Get a copy of the byte array created thus far
	 * 
	 * @return		A copy of the byte array stored in this buffer
	 */
	public byte[] getByteArray() {
		byte[] retval = new byte[dataLength];
		System.arraycopy(buffer, 0, retval, 0, dataLength);
		return retval;
	}

	/**
	 * Reset the object so it can be reused.
	 * 
	 */
	public void clear() {
		if (buffer.length > MAX_SIZE_TO_SAVE) {
			buffer = new byte[DEFAULT_INITIAL_ALLOCATION];
		}
		dataLength = 0;
	}

	public void clear(byte[] initialContents) {
		clear();
		addToBufferNoLength(initialContents);
	}

	public int getCurrentLength() {
		return dataLength;
	}
}