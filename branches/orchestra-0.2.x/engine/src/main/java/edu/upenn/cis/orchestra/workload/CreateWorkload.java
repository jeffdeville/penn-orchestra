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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Subtuple;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.reconciliation.Benchmark;
import edu.upenn.cis.orchestra.reconciliation.Db;

public class CreateWorkload {
	enum TrustModel { SOME, SAME }
	enum PrioMode { SAME, DIFFERENT }
	enum DataModel { ZIPFIAN, UNIFORM }
	TrustModel trustModel = TrustModel.SAME;
	PrioMode prioMode = PrioMode.SAME;
	DataModel dataModel = DataModel.ZIPFIAN;
	String outputFile = "workload";
	Random random = new Random();
	int numCycles = 10;

	int totalNumPeers = 10;
	int startingPeerId = 0;	
	int trustNum = 10;
	int numBrowsers = 0;
	int numUpdaters = 10;

	int txnSize = 5;

	double reconcileProb = 0.2;
	double resolveProb = 0.2;
	// Only for updaters, this is always 0.0 for browsers
	double txnProb = 0.2;
	
	double zipfian = 1.0;
	
	String envdir = null;
	
	LockManagerClient lmc = null;
	
	private static class DbData {
		String organism;
		ArrayList<String> dbs;
		ArrayList<String> ids;
		DbData(String organism, ArrayList<String> dbs, ArrayList<String> ids) {
			this.organism = organism;
			this.dbs = dbs;
			this.ids = ids;
		}
	}
	
	Schema schema;
	Relation funcSchema;
	Relation xrefSchema;
	RandomizedSet ids;
	RandomizedSet funcs;
	HashMap<String,DbData> recs;
	
	
	public void createWorkload(DatabaseFactory dbf, String[] args) throws Exception {
		parseOpts(args);
		
		int numPeers = numBrowsers + numUpdaters;
		
		int[] localPeers = new int[numPeers];
		
		for (int i = 0; i < numPeers; ++i) {
			localPeers[i] = i + startingPeerId;
		}
		
		recs = new HashMap<String,DbData>();
		ids = new RandomizedSet(false, random);
		funcs = new RandomizedSet(true, random);
		
		parseDataFiles(new FileInputStream("entries.txt"), new FileInputStream("func.txt"),
				recs, ids, funcs, 1000);
		
		if (dataModel == DataModel.ZIPFIAN) {
			ids.makeWeightedZipfian(zipfian);
		}
				
		schema = new Schema(getClass().getSimpleName() + "_schema");
		funcSchema = schema.addRelation("func");
		funcSchema.addCol("protein", new StringType(true, false, true, 11));
		funcSchema.addCol("organism", new StringType(false, false, true, 120));
		funcSchema.addCol("function", new StringType(false, false, true, 80));
		xrefSchema = schema.addRelation("xref");
		xrefSchema.addCol("protein", new StringType(true, false, false, 11));
		xrefSchema.addCol("db", new StringType(true, false, true, 20));
		xrefSchema.addCol("id", new StringType(true, false, true, 50));
		schema.markFinished();

		// Modified values only change function field
		ArrayList<Integer> updatePos = new ArrayList<Integer>();
		updatePos.add(2);
		
		FileOutputStream fos = new FileOutputStream(outputFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);

		oos.writeObject(schema);
		oos.writeObject(startingPeerId);
		oos.writeObject(numPeers);
		
		if (envdir == null) {
			envdir = "cw" + startingPeerId;
		}
		
		File f = new File(envdir);
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}

		
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setTransactional(true);
		ec.setAllowCreate(true);
		File envFile = new File(envdir);
		Environment e = new Environment(envFile, ec);
		
		dbf.initDb(schema);
		
		HashMap<Integer,Db> dbs = new HashMap<Integer,Db>();

		for (int id : localPeers) {
			dbs.put(id, dbf.createDb(id, schema, e));
			dbs.get(id).setBenchmark(new Benchmark());
		}

		// TODO: figure out how we want to record these
		// Create trust predicates
		/*
		if (trustModel == TrustModel.SOME) {
			// Create a new trusted set of size trustNum for each peer
			for (int id : localPeers) {
				List<Integer> prios;
				if (prioMode == PrioMode.SAME) {
					prios = createSamePrio(trustNum);
				} else {
					prios = createRandomPrios(trustNum);
				}
				Iterator<Integer> prioIter = prios.iterator();
				Set<Integer> trustedPeers = buildSet(totalNumPeers - 1, trustNum, id);
				for (int trustedPeer : trustedPeers) {
					int prio = prioIter.next();
					WorkloadAction wa = new TrustConditionAction(id, trustedPeer, "func", null, prio);
					oos.writeObject(wa);
					wa.doAction(dbs, lmc);
					wa = new TrustConditionAction(id, trustedPeer, "xref", null, prio);
					oos.writeObject(wa);
					wa.doAction(dbs, lmc);
				}
			}
		} else if (trustModel == TrustModel.SAME) {
			// Use the same trusted set for each peer
			Set<Integer> trustedPeers = buildSet(totalNumPeers - 1, trustNum, -1);
			List<Integer> prios;
			if (prioMode == PrioMode.SAME) {
				prios = createSamePrio(trustNum);
			} else {
				prios = createRandomPrios(trustNum);
			}
			for (int id : localPeers) {
				Iterator<Integer> prioIter = prios.iterator();
				for (int trustedPeer : trustedPeers) {
					int prio = prioIter.next();
					if (id != trustedPeer) {
						WorkloadAction wa = new TrustConditionAction(id, trustedPeer, "func", null, prio);
						oos.writeObject(wa);
						wa.doAction(dbs, lmc);
						wa = new TrustConditionAction(id, trustedPeer, "xref", null, prio);
						oos.writeObject(wa);
						wa.doAction(dbs, lmc);
					}
				}
			}
		}
		*/

		CreateTuple ct = new CreateTuple();
		
		ArrayList<WorkloadPeer> wps = new ArrayList<WorkloadPeer>();
		
		
		for (int id : localPeers) {
			if ((id - startingPeerId) < numBrowsers) {
				wps.add(new WorkloadPeer(id, dbs.get(id), random, reconcileProb, resolveProb, 0.0, txnSize, ct));
			} else {
				wps.add(new WorkloadPeer(id, dbs.get(id), random, reconcileProb, resolveProb, txnProb, txnSize, ct));
			}
		}
		
		PrintStream status = System.err;
		
		for (int i = 0; i < numCycles; ++i) {
			status.print("Round " + i + ":");
			status.flush();
			for (WorkloadPeer wp : wps) {
				WorkloadAction wa = wp.chooseAction();
				if (wa == null) {
					status.print('-');
				} else if (wa instanceof ReconcileAction) {
					status.print('r');
				} else if (wa instanceof ResolveAction) {
					status.print('R');
				} else if (wa instanceof TransactionAction) {
					status.print('T');
				}
				status.flush();
				if (wa != null) {
					wa.doAction(dbs, lmc);
					oos.writeObject(wa);
				}
				status.print('.');
			}
			status.println();
		}
		
		HashMap<Subtuple,Set<Tuple>> stateForRatio = new HashMap<Subtuple,Set<Tuple>>();
		HashMap<Subtuple,Integer> numPeersForKey = new HashMap<Subtuple,Integer>();
		
		System.out.println("# Recons\tRecon Time\tRecon Net Time\tResolve Time");
		
		int numHits = 0;
		int numRequests = 0;
		
		ArrayList<Integer> numHitsDistribution = new ArrayList<Integer>();
		
		for (int id: localPeers) {
			int numRecons = dbs.get(id).getCurrentRecno();
			Benchmark b = dbs.get(id).getBenchmark();
			double reconTime = (b.reconcile + b.recordReconcile + b.getReconciliationData) / 1000000000.0;
			double reconNetTime = (b.recordReconcileNet + b.getReconciliationDataNet) / 1000000000.0;
			double resolveTime = b.resolveConflicts / 1000000000.0;
			System.out.println(numRecons + "\t" + reconTime + "\t" + reconNetTime + "\t" + resolveTime);
			Map<String,TupleSet> state = dbs.get(id).getState();
			oos.writeObject(state);
			Set<Tuple> funcState = state.get("func");
			for (Tuple t : funcState) {
				Subtuple key = t.getKeySubtuple();
				if (! numPeersForKey.containsKey(key)) {
					numPeersForKey.put(key, 0);
				}
				numPeersForKey.put(key,numPeersForKey.get(key) + 1);
				Set<Tuple> tupleSet = stateForRatio.get(t);
				if (tupleSet == null) {
					tupleSet = new HashSet<Tuple>();
					stateForRatio.put(key,tupleSet);
				}
				tupleSet.add(t);
			}
			
		}
		int numKeys = stateForRatio.size();
		int numVals = 0;
		int numValsModified = 0;
		

		for (Map.Entry<Subtuple,Set<Tuple>> entry : stateForRatio.entrySet()) {
			numVals += entry.getValue().size();
			numValsModified += entry.getValue().size();
			if (numPeersForKey.get(entry.getKey()) < numPeers) {
				// TODO: what does this do?
				++numValsModified;
			}
		}
		
		double stateRatio = ((double) numVals) / numKeys;
		double modifiedStateRatio = ((double) numValsModified) / numKeys;
		
		System.out.println("State ratio is: " + stateRatio);
		System.out.println("Modified state ratio is: " + modifiedStateRatio);
		System.out.println("Number of keys in database is: " + numKeys);
		
		System.out.println(numHits + " numHits / " + numRequests + " numRequests = " + (((double) numHits) / numRequests));

		int newNumRequests = 0;
		int newNumHits = 0;
		
		for (int i = 0 ; i < numHitsDistribution.size(); ++i) {
			int nh = numHitsDistribution.get(i);
			newNumRequests += nh;
			newNumHits += i * nh;
		}
		System.out.println("Distribution: " + numHitsDistribution + " (" + (((double) newNumHits) / newNumRequests) + ")");
		
		
//		System.out.println(ids);
		
		// I don't quite get why this is necessary, but otherwise I seem to get
		// exceptions when readObject hits the end of the file; when this is added
		// it never actually gets there, and therefore works.
		oos.writeObject("END OF FILE");
		oos.close();
		
		for (Db db : dbs.values()) {
			db.disconnect();
		}
		
		e.close();
	}
	
	private void parseOpts(String args[]) {
			int numOpts = args.length;
			boolean bad = false;
			for (int i = 0; i < numOpts; ++i) {
				if (i == (numOpts - 1)) {
					// All of the following arguments need a value following them
					bad = true;
					break;
				}
				if (args[i].equals("-output")) {
					outputFile = args[i+1];
				} else if (args[i].equals("-trustmodel")) {
					if (args[i+1].equals("some")) {
						trustModel = TrustModel.SOME;
					} else if (args[i+1].equals("same")) {
						trustModel = TrustModel.SAME;
					} else {
						System.err.println("Unknown trust model: " + args[i+1]);
						bad = true;
						break;
					}
				} else if (args[i].equals("-priomode")) {
					if (args[i+1].equals("same")) {
						prioMode = PrioMode.SAME;
					} else if (args[i+1].equals("different")) {
						prioMode = PrioMode.DIFFERENT;
					} else {
						System.err.println("Unknown priority mode: " + args[i+1]);
						bad = true;
						break;
					}
				} else if (args[i].equals("-datamodel")) {
					if (args[i+1].equals("uniform")) {
						dataModel = DataModel.UNIFORM;
					} else if (args[i+1].equals("zipfian")) {
						dataModel = DataModel.ZIPFIAN;
					} else {
						System.err.println("Uknown data model: " + args[i+1]);
						bad = true;
						break;
					}
				} else if (args[i].equals("-trusted")) {
					trustNum = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-random")) {
					random = new Random(Long.parseLong(args[i+1]));
				} else if (args[i].equals("-numcycles")) {
					numCycles = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-numbrowsers")) {
					numBrowsers = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-numupdaters")) {
					numUpdaters = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-txnsize")) {
					txnSize = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-zipfian")) {
					zipfian = Double.parseDouble(args[i+1]);
				} else if (args[i].equals("-resolveprob")) {
					resolveProb = Double.parseDouble(args[i+1]);
				} else if (args[i].equals("-reconcileprob")) {
					reconcileProb = Double.parseDouble(args[i+1]);
				} else if (args[i].equals("-txnprob")) {
					txnProb = Double.parseDouble(args[i+1]);
				} else if (args[i].equals("-startid")) {
					startingPeerId = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-totalpeers")) {
					totalNumPeers = Integer.parseInt(args[i+1]);
				} else if (args[i].equals("-env")) {
					envdir = args[i+1];
				} else if (args[i].equals("-lockmanager")) {
					lmc = new LockManagerClient(args[i+1]);
				} else {
					bad = true;
					break;
				}
				++i;
			}
		if (bad) {
			System.err.println("Command line options: -output file -startid n -totalpeers n -random seed -trustmodel [some|same] -priomode [same|different] -trusted n -numcycles n -numbrowsers n -numupdaters n -txnsize n -datamodel [uniform|zipfian] -zipfian f -env dir -lockmanager hostname");
			System.exit(-1);
		}
		if (trustNum > numBrowsers + numUpdaters) {
			System.err.println("Number of trusted peers can be at most the total number of peers");
			System.exit(-2);
		}
	}
	
	/**
	 * Parse the data files generated from SwissProt
	 * 
	 * @param entries		An input stream reading the entries.txt file
	 * @param func			An input stream reading the func.txt file
	 * @param recs			The HashMap to hold the entry data parsed from the DB records
	 * @param ids			A randomized non-weighted set to hold the record IDs
	 * @param funcs			A randomized weighted set to hold the function records
	 * @param numRecs		The number of records to read in
	 */
	private void parseDataFiles(InputStream entries, InputStream func,
			HashMap<String,DbData> recs, RandomizedSet ids, RandomizedSet funcs,
			int numRecs) throws IOException {
		BufferedReader entriesReader = new BufferedReader(new InputStreamReader(entries));
		BufferedReader funcReader = new BufferedReader(new InputStreamReader(func));
		
		String line;
		
		int linesRead = 0;
		
		while ((line = entriesReader.readLine()) != null) {
			String[] entriesLine = line.split("\t");
			ArrayList<String> dbs = new ArrayList<String>((entriesLine.length / 2) - 1);
			ArrayList<String> dbIds = new ArrayList<String>(dbs.size());
			for (int i = 2; i < entriesLine.length; i += 2) {
				dbs.add(entriesLine[i]);
				dbIds.add(entriesLine[i+1]);
			}
			DbData dd = new DbData(entriesLine[1], dbs, dbIds);
			recs.put(entriesLine[0], dd);
			ids.addElement(entriesLine[0]);
			++linesRead;
			if (linesRead >= numRecs) {
				break;
			}
		}
		
		while ((line = funcReader.readLine()) != null) {
			String[] funcLine = line.split("\t");
			funcs.addElementWithWeight(funcLine[0], Integer.parseInt(funcLine[1]));
		}
		entriesReader.close();
		funcReader.close();
	}

	
	/**
	 * Build a set of randomly chosen integers between <code>0</code> and <code>max</code>
	 * 
	 * @param max		the maximum value to appear in the set
	 * @param num		the number of values to appear in the set
	 * @param exclude	a value to exclude from the set, or <code>-1</code>
	 * 					to exclude no values
	 * @return			the randomly generated set
	 */
	private Set<Integer> buildSet(int max, int num, int exclude) {
		HashSet<Integer> retval = new HashSet<Integer>();
		
		if (num * 2 > max) {
			// Start with full set and remove elements
			for (int i = 0; i <= max; ++i) {
				if (i != exclude) {
					retval.add(i);
				}
			}
			while (retval.size() > num) {
				int val = random.nextInt(max+1);
				retval.remove(val);
			}
		} else {
			// Start with empty set and add elements
			while (retval.size() > num) {
				int val = random.nextInt(max+1);
				if (val != exclude) {
					retval.add(val);
				}
			}
		}
		
		return retval;
	}
	
	/**
	 * Create a list containing the numbers 1 to <code>max</code> in a
	 * random order
	 * 
	 * @param max		The maximum number to include in the list
	 * @return			The random permutation of [1,max]
	 */
	private List<Integer> createRandomPrios(int max) {
		ArrayList<Integer> values = new ArrayList<Integer>();

		for (int i = 1; i <= max; ++i) {
			values.add(i);
		}

		Collections.shuffle(values, random);
		
		return values;
	}
	
	/**
	 * Create a list containing <code>num</code> copies of the same number
	 * 
	 * @param num		The number of copies
	 * @return			The list
	 */
	private static List<Integer> createSamePrio(int num) {
		ArrayList<Integer> values = new ArrayList<Integer>();
		for (int i = 0; i < num; ++i) {
			values.add(1);
		}
		
		return values;
	}
	
	public interface DatabaseFactory {
		Db createDb(int peerID, Schema s, Environment env) throws Exception;
		void initDb(Schema s) throws Exception;
	}
	
	private class CreateTuple implements TupleGenerator {
		public Tuple getTuple() throws Exception {
			String id = ids.chooseElement();
			DbData dd = recs.get(id);
			Tuple t = new Tuple(funcSchema);
			t.set(0, id);
			t.set(1, dd.organism);
			t.set(2, funcs.chooseElement());
			return t;
		}

		public List<Tuple> getSupportingTuples(Tuple t) throws Exception {
			String id = (String) t.get(0);
			DbData dd = recs.get(id);

			ArrayList<Tuple> supportingTuples = new ArrayList<Tuple>(dd.dbs.size());
			
			final int numDbs = dd.dbs.size();
			
			for (int i = 0; i < numDbs; ++i) {
				Tuple xrefTuple = new Tuple(xrefSchema);
				xrefTuple.set(0, id);
				xrefTuple.set(1, dd.dbs.get(i));
				xrefTuple.set(2, dd.ids.get(i));
				supportingTuples.add(xrefTuple);
			}
			
			return supportingTuples;
		}
		
	}
}
