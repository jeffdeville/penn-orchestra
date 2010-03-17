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

import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;

public class ByteBufferReader {
	static final int intBytes = IntType.bytesPerInt;
	// private Schema s;
	private ISchemaIDBinding s;
	private byte[] bytes;
	private int offset;
	private int length;

	public ByteBufferReader(ISchemaIDBinding s, byte[] bytes, int offset,
			int length) {
		this.s = s;
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
	}

	public ByteBufferReader(ISchemaIDBinding s, byte[] bytes) {
		this(s, bytes, 0, bytes.length);
	}

	public ByteBufferReader(ISchemaIDBinding s) {
		this.s = s;
		this.bytes = null;
		this.offset = 0;
		this.length = 0;
	}

	public ByteBufferReader(byte[] bytes) {
		this(null, bytes);
	}

	public ByteBufferReader() {
		this((ISchemaIDBinding) null);
	}

	public void reset(byte[] bytes) {
		reset(bytes, 0, bytes.length);
	}

	public void reset(byte[] bytes, int offset, int length) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
	}

	public boolean readBoolean() {
		return (readByte() > 0);
	}

	public AbstractPeerID readPeerID() {
		int idLength = readInt();
		if (length == 0) {
			return null;
		}

		checkState();
		AbstractPeerID pid = AbstractPeerID.fromBytes(bytes, offset, idLength);
		offset += idLength;
		length -= idLength;
		return pid;
	}

	public byte readByte() {
		checkState();
		byte retval = bytes[offset];
		++offset;
		--length;
		return retval;
	}

	public int readInt() {
		checkState();
		int value = IntType.getValFromBytes(bytes, offset);
		offset += intBytes;
		length -= intBytes;
		return value;
	}

	public Relation readRelationFromId() {
		int relationId = readInt();
		return s.getRelationFor(relationId);
	}

	public String readString() {
		byte[] bytes = readByteArray();
		if (bytes == null) {
			return null;
		} else {
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(
						"Couldn't convert String from UTF-8", e);
			}
		}
	}

	public Tuple getTupleFromBytes(ISchemaIDBinding sch, byte[] bytes,
			int offset, int length) {
		final int bytesPerInt = IntType.bytesPerInt;
		int relId = IntType.getValFromBytes(bytes, offset);
		Relation r = sch.getRelationFor(relId);

		if (r == null) {
			System.err.println("Unable to find relation ID " + relId);
			return null;
		}
		// if (IDToIndex.get(relId) == null)
		// return null;
		// return new RelationTuple(relationSchemas.get(IDToIndex.get(relId)/* -
		// relOffset*/), bytes, offset + bytesPerInt, length - bytesPerInt);

		return new Tuple(r, bytes, offset + bytesPerInt, length - bytesPerInt);
	}

	public Tuple readTuple() {
		int tupleLength = readInt();
		if (tupleLength < 0) {
			return null;
		}
		checkState();
		Tuple t = getTupleFromBytes(s, bytes, offset, tupleLength);
		offset += tupleLength;
		length -= tupleLength;
		return t;
	}

	public TxnPeerID readTxnPeerID() {
		int tpiLength = readInt();
		if (tpiLength < 0) {
			return null;
		}
		checkState();
		TxnPeerID tpi = TxnPeerID.fromBytes(bytes, offset, tpiLength);
		offset += tpiLength;
		length -= tpiLength;
		return tpi;
	}

	public Update readUpdate() {
		int updateLength = readInt();
		if (updateLength < 0) {
			return null;
		}
		checkState();
		Update u = Update.fromBytes(s, bytes, offset, updateLength);
		offset += updateLength;
		length -= updateLength;
		return u;
	}

	public byte[] readByteArray() {
		int arrayLength = readInt();
		if (arrayLength < 0) {
			return null;
		}
		byte[] retval = new byte[arrayLength];
		System.arraycopy(bytes, offset, retval, 0, arrayLength);
		offset += arrayLength;
		length -= arrayLength;
		return retval;
	}

	public byte[] readByteArrayNoLength(int arrayLength) {
		byte[] retval = new byte[arrayLength];
		System.arraycopy(bytes, offset, retval, 0, arrayLength);
		offset += arrayLength;
		this.length -= arrayLength;
		return retval;
	}

	public PidAndRecno readPidAndRecno() {
		AbstractPeerID pid = readPeerID();
		int recno = readInt();
		return new PidAndRecno(pid, recno);
	}

	/**
	 * Get a ByteBufferReader for a byte array that was written to this
	 * ByteBuffer using the <code>addToBuffer(byte[])</code> method.
	 * 
	 * @return The new reader
	 */
	public ByteBufferReader getSubReader() {
		int subLength = readInt();
		ByteBufferReader retval = new ByteBufferReader(s, bytes, offset,
				subLength);
		offset += subLength;
		length -= subLength;
		return retval;
	}

	private void checkState() {
		if (length <= 0) {
			throw new IllegalStateException(
					"Attempt to read from buffer with no bytes to be read");
		}
	}

	public boolean hasFinished() {
		return length == 0;
	}

	public int getCurrentOffset() {
		return offset;
	}

	public int getLengthRemaining() {
		return length;
	}

	/*
	 * public void setSchema(Schema s) { this.s = s; }
	 */
}
