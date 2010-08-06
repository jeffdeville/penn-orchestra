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

public class ReconciliationEpoch {
	public final int recno;
	public final int epoch;
	
	public ReconciliationEpoch(int recno, int epoch) {
		this.recno = recno;
		this.epoch = epoch;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		ReconciliationEpoch re = (ReconciliationEpoch) o;
		return (recno == re.recno && epoch == re.epoch);
	}
	
	public int hashCode() {
		return recno + 37 * epoch;
	}
	
	public String toString() {
		return "(" + recno + "," + epoch + ")";
	}
}
