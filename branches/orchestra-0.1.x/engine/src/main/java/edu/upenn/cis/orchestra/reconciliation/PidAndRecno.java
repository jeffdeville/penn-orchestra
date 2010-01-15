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
package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;

public class PidAndRecno {
	private final AbstractPeerID pid;
	private final int recno;
	
	public PidAndRecno(AbstractPeerID pid, int recno) {
		this.pid = pid;
		this.recno = recno;
	}
	
	public PidAndRecno(byte[] bytes) {
		ByteBufferReader bbr = new ByteBufferReader(null, bytes);
		pid = bbr.readPeerID();
		recno = bbr.readInt();
		if (! bbr.hasFinished()) {
			throw new IllegalArgumentException("Byte array contains extra data after PidAndRecno");
		}
	}
	
	public byte[] getBytes() {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(pid);
		bbw.addToBuffer(recno);
		return bbw.getByteArray();
	}
	
	public AbstractPeerID getPid() {
		return pid;
	}
	
	public int getRecno() {
		return recno;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		PidAndRecno par = (PidAndRecno) o;
		return (recno == par.recno && pid.equals(par.pid));
	}
	
	public int hashCode() {
		return recno + 37 * pid.hashCode();
	}
	
	public String toString() {
		return pid + "r" + recno;
	}
}
