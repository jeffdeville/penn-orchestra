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

/**
 * Class to hold benchmark data
 * 
 * 
 * @author Nick
 *
 */
public class Benchmark {
	// All are in nanoseconds
	public long publish;
	public long publishNet;
	public long publishServer;
	public long recordTxnDecisions;
	public long recordTxnDecisionsNet;
	public long recordTxnDecisionsServer;
	public long recordReconcile;
	public long recordReconcileNet;
	public long recordReconcileServer;
	public long getReconciliationData;
	public long getReconciliationDataNet;
	public long getReconciliationDataServer;
	public long getCurrentRecno;
	public long getCurrentRecnoNet;
	public long getCurrentRecnoServer;
	public long resolveConflicts;
	public long reconcile;
	public long getTxnStatusNet;
	public long getTxnStatusServer;
	
	
	
	public Benchmark() {
		publish = 0;
		publishNet = 0;
		publishServer = 0;
		recordTxnDecisions = 0;
		recordTxnDecisionsNet = 0;
		recordTxnDecisionsServer = 0;
		recordReconcile = 0;
		recordReconcileNet = 0;
		recordReconcileServer = 0;
		getReconciliationData = 0;
		getReconciliationDataNet = 0;
		getReconciliationDataServer = 0;
		getCurrentRecno = 0;
		getCurrentRecnoNet = 0;
		getCurrentRecnoServer = 0;
		resolveConflicts = 0;
		reconcile = 0;
		getTxnStatusNet = 0;
		getTxnStatusServer = 0;
	}
	
	public static String getHeaders() {
		return "publish\tpublishNet\tpublishServer\trecordTxnDecisions\trecordTxnDecisionsNet" +
		"\trecordTxnDecisionsServer\trecordReconcile\trecordReconcileNet\trecordReconcileServer\t" +
		"getReconciliationData\tgetReconciliationDataNet\tgetReconciliationDataServer\t" +
		"getCurrentRecno\tgetCurrentRecnoNet\tgetCurrentRecnoServer\tresolveConflicts\t" +
		"reconcile\tgetTxnStatusNet\tgetTxnStatusServer";
	}
	
	public String toString() {
		return publish + "\t" + publishNet + "\t" + publishServer + "\t" +
		recordTxnDecisions + "\t" + recordTxnDecisionsNet + "\t" + recordTxnDecisionsServer + "\t" +
		recordReconcile + "\t" + recordReconcileNet + "\t" + recordReconcileServer + "\t" +
		getReconciliationData + "\t" + getReconciliationDataNet + "\t" + getReconciliationDataServer + "\t" +
		getCurrentRecno + "\t" + getCurrentRecnoNet + "\t" + getCurrentRecnoServer + "\t" +
		resolveConflicts + "\t" + reconcile + "\t" + getTxnStatusNet + "\t" + getTxnStatusServer;
	}
}