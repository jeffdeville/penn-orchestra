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

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

public class Decision implements Serializable, Comparable<Decision> {
	private static final long serialVersionUID = 1L;
	public final TxnPeerID tpi;
	public final int recno;
	public final boolean accepted;
	
	public Decision(TxnPeerID tpi, int recno, boolean accepted) {
		this.tpi = tpi.duplicate();
		this.recno = recno;
		this.accepted = accepted;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		Decision d = (Decision) o;
		
		return (recno == d.recno && accepted == d.accepted && tpi.equals(d.tpi));
	}
	
	public int hashCode() {
		return tpi.hashCode() + 37 * recno;
	}
	
	public String toString() {
		return tpi + (accepted ? " accepted " : " rejected ") + "at " + recno;
	}

	public int compareTo(Decision d) {
		if (d.recno != recno) {
			return recno - d.recno;
		} else if (d.accepted ^ accepted) {
			int otherVal = d.accepted ? 0 : 1;
			int thisVal = accepted ? 0 : 1;
			return thisVal - otherVal;
		} else {
			return 0;
		}

	}
	
	
}
