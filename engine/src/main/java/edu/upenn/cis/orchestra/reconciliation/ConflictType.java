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

import edu.upenn.cis.orchestra.datamodel.Update;


public abstract class ConflictType {

	// UPDATE = multiple different updates to same value
	// INITIAL = same insertion becomes different values (after flattening)
	// KEY = key violation (same key, different values)
	public enum ConflictTypeCode { UPDATE, INITIAL, KEY }
	
	public abstract ConflictTypeCode getTypeCode();
	
	/**
	 * Returns true if the conflict this ConflictType stands for
	 * should involve the update <code>u</code>
	 * 
	 * @param u			The update to check
	 * @return			<code>true</code> if the update should participate in
	 * 					this conflict, <code>false</code> if it should not
	 */
	public abstract boolean isApplicableFor(Update u);
	
	public static ConflictTypeCode getConflictType(Update u1, Update u2) throws DbException {
		if (!u1.getRelationID().equals(u2.getRelationID())) {
			return null;
		}
		boolean u1n = u1.isNull(), u2n = u2.isNull();
		if (u1.getNewVal() != null && u2.getNewVal() != null && u1.getNewVal().sameKey(u2.getNewVal()) && (! u1.getNewVal().equals(u2.getNewVal()))) {
			// insertion or modification to different values, same key
			return ConflictTypeCode.KEY;
		} else if (u1.isInsertion() && u2.isDeletion() && u1.getNewVal().sameKey(u2.getOldVal())) {
			// different ops, same key
			throw new DbException("Should not have insertion and deletion of same key");
		} else if (u2.isInsertion() && u1.isDeletion() && u2.getNewVal().sameKey(u1.getOldVal())) {
			// different ops, same key
			throw new DbException("Should not have insertion and deletion of same key");
		} else if (u1.getOldVal() != null && u2.getOldVal() != null && u1.getOldVal().equals(u2.getOldVal()) && (u1.getNewVal() != null || u2.getNewVal() != null) && (u1.getNewVal() == null || u2.getNewVal() == null || (! u1.getNewVal().equals(u2.getNewVal())))) {
			// same value => different values (including deletion)
			return ConflictTypeCode.UPDATE;
		} else if (u1.getInitialVal() != null && u1.getInitialTid() != null && u1.getInitialTid().equals(u2.getInitialTid()) && u1.getInitialVal().equals(u2.getInitialVal()) && ((u1n && (! u2n)) || (u2n && (! u1n)) || (! u1n) && (! u2n) && (!u1.getNewVal().equals(u2.getNewVal())))) {
			// same initial value & tid, different final values (including no final value)
			return ConflictTypeCode.INITIAL;
		} else {
			return null;
		}
	}
}
