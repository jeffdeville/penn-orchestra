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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StreamOutputBuffer extends OutputBuffer {
	private DataOutputStream out;

	public StreamOutputBuffer(OutputStream os) {
		out = new DataOutputStream(os);
	}

	public void close() throws IOException {
		out.close();
	}
	
	@Override
	public void writeBoolean(boolean bool) {
		try {
			out.writeBoolean(bool);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeBytesNoLength(byte[] bytes) {
		try {
			out.write(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeBytesNoLength(byte[] bytes, int offset, int length) {
		try {
			out.write(bytes, offset, length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeInt(int v) {
		try {
			out.writeInt(v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeLong(long l) {
		try {
			out.writeLong(l);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeShort(short s) {
		try {
			out.writeShort(s);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return out.size();
	}
}
