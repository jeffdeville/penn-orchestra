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

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;



public class InitialConflict extends ConflictType {
	TxnPeerID initialTid;
	Tuple initialVal;
	
	public InitialConflict(TxnPeerID initialTid, Tuple initialVal) {
		this.initialTid = initialTid.duplicate();
		this.initialVal = initialVal.duplicate();
	}

	public ConflictTypeCode getTypeCode() {
		return ConflictTypeCode.INITIAL;
	}

	public boolean isApplicableFor(Update u) {
		return (initialTid.equals(u.getInitialTid()) && initialVal.equals(u.getInitialVal()));
	}

}
