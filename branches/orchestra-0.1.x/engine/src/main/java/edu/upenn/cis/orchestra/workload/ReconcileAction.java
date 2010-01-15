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
package edu.upenn.cis.orchestra.workload;

import edu.upenn.cis.orchestra.reconciliation.Db;

import java.util.Map;

public class ReconcileAction extends WorkloadAction {
	private static final long serialVersionUID = 1L;

	public ReconcileAction(int peer) {
		super(peer);
	}

	protected void doAction(Map<Integer,Db> dbs, LockManagerClient lmc) throws Exception {
		if (lmc != null) {
			lmc.getLock(peer);
		}
		dbs.get(peer).publish();
		dbs.get(peer).reconcile();
		if (lmc != null) {
			lmc.releaseLock();
		}
	}

	public String toString() {
		return "Peer " + peer + ": reconcile";
	}

}
