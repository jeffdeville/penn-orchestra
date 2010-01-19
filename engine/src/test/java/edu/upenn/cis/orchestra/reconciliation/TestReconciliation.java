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

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.TestUtil.BROKEN_TESTNG_GROUP;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringPeerID;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
public abstract class TestReconciliation extends TestCase {
	
	protected static final String SCHEMA_NAME = "test";
	protected ArrayList<Db> dbs;
	Schema s;
	private ISchemaIDBinding scm;
	OrchestraSystem sys;
	Relation rs;
	Tuple tN1, tN2, tN3, tN4, tN5, tM4, tM5, tNull;
	Update insN1, insN2, insN3, modN1N3, modN1N2, modN1N4, modN2N5, insM4, insM5, modM4M5, modN1M5, delN1, delM5, insNull;
	ArrayList<AbstractPeerID> peers;
	ArrayList<TrustConditions> tcs;
	UpdateStore.Factory factory;
	
	static protected final int numPeers = 5;
	protected List<Schema> schemas;
	protected Map<AbstractPeerID,Integer> peerMap;
	
	protected abstract UpdateStore.Factory getStoreFactory();
	protected abstract void clearState(Schema s) throws Exception;
	
	private void setAllTrusted() throws Exception {
		tcs = new ArrayList<TrustConditions>(numPeers);
		for (int i = 0; i < numPeers; ++i) {
			tcs.add(new TrustConditions(peers.get(i)));
		}
		
		tcs.get(0).addTrustCondition(peers.get(1), s, "R", null, 1);
		tcs.get(0).addTrustCondition(peers.get(2), s, "R", null, 1);
		tcs.get(0).addTrustCondition(peers.get(3), s, "R", null, 1);
		tcs.get(1).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(1).addTrustCondition(peers.get(2), s, "R", null, 1);
		tcs.get(1).addTrustCondition(peers.get(3), s, "R", null, 1);
		tcs.get(2).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(2).addTrustCondition(peers.get(1), s, "R", null, 1);
		tcs.get(2).addTrustCondition(peers.get(3), s, "R", null, 1);
		tcs.get(3).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(3).addTrustCondition(peers.get(1), s, "R", null, 1);
		tcs.get(3).addTrustCondition(peers.get(2), s, "R", null, 1);

		dbs = new ArrayList<Db>(numPeers);
		for (int i = 0; i < numPeers; ++i) {
			dbs.add(new ClientCentricDb(sys, scm, s, new StringPeerID("p"), tcs.get(i), factory, HashTableStore.FACTORY));
		}
	}
	
	private void setSomeTrusted() throws Exception {
		tcs = new ArrayList<TrustConditions>(numPeers);
		for (int i = 0; i < numPeers; ++i) {
			tcs.add(new TrustConditions(peers.get(i)));
		}
		
		tcs.get(1).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(2).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(3).addTrustCondition(peers.get(1), s, "R", null, 1);
		tcs.get(3).addTrustCondition(peers.get(2), s, "R", null, 1);
		tcs.get(4).addTrustCondition(peers.get(0), s, "R", null, 1);
		tcs.get(4).addTrustCondition(peers.get(1), s, "R", null, 1);
		tcs.get(4).addTrustCondition(peers.get(2), s, "R", null, 1);

		dbs = new ArrayList<Db>(numPeers);
		for (int i = 0; i < numPeers; ++i) {
			dbs.add(new ClientCentricDb(sys, scm, s, new StringPeerID("p"), tcs.get(i), factory, HashTableStore.FACTORY));
		}
	}
	
	@Override
	@BeforeMethod
	protected void setUp() throws Exception {
		super.setUp();

		Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap();

		factory = getStoreFactory();
		s = new Schema(getClass().getSimpleName() + "_schema");
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(false, false, true, 10));
		rs.addCol("val", new IntType(false,false));
		rs.setPrimaryKey(new PrimaryKey("pk", rs, Collections.singleton("name")));
		s.markFinished();
		try {
			clearState(s);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		schemas = new ArrayList<Schema>();
		schemas.add(s);
		
		peerMap = new HashMap<AbstractPeerID,Integer>();
		
		peers = new ArrayList<AbstractPeerID>(numPeers);
		for (int i = 0; i < 3; ++i) {
			peers.add(new IntPeerID(i));
			peerMap.put(peers.get(i), 0);
			peerIDToSchema.put(peers.get(i), s);
		}
		for (int i = 3; i < numPeers; ++i) {
			peers.add(new StringPeerID("Peer #" + i));
			peerMap.put(peers.get(i), 0);
			peerIDToSchema.put(peers.get(i), s);
		}
		scm = new LocalSchemaIDBinding(peerIDToSchema);
		sys = new OrchestraSystem(scm);
		for (int i = 0; i < 3; ++i) {
			sys.addPeer(new Peer(String.valueOf(i), "", ""));
		}
		for (int i = 3; i < numPeers; ++i) {
			sys.addPeer(new Peer("Peer #" + String.valueOf(i), "", ""));
		}
		
		tN1 = new Tuple(rs);
		tN1.set("name", "Nick");
		tN1.set("val", 1);
		tN2 = new Tuple(rs);
		tN2.set("name", "Nick");
		tN2.set("val", 2);
		tN3 = new Tuple(rs);
		tN3.set("name", "Nick");
		tN3.set("val", 3);
		tN4 = new Tuple(rs);
		tN4.set("name", "Nick");
		tN4.set("val", 4);
		tN5 = new Tuple(rs);
		tN5.set("name", "Nick");
		tN5.set("val", 5);
		tM4 = new Tuple(rs);
		tM4.set("name", "Mark");
		tM4.set("val", 4);
		tM5 = new Tuple(rs);
		tM5.set("name", "Mark");
		tM5.set("val", 5);
		tNull = new Tuple(rs);
		tNull.set("name", "Fred");
		tNull.setLabeledNull("val", 999);

		insN1 = new Update(null, tN1);
		insN2 = new Update(null, tN2);
		insN3 = new Update(null, tN3);
		modN1N3 = new Update(tN1,tN3);
		modN1N2 = new Update(tN1,tN2);
		modN1N4 = new Update(tN1, tN4);
		modN2N5 = new Update(tN2, tN5);
		insM4 = new Update(null, tM4);
		insM5 = new Update(null, tM5);
		modM4M5 = new Update(tM4, tM5);
		modN1M5 = new Update(tN1, tM5);
		delN1 = new Update(tN1, null);
		delM5 = new Update(tM5, null);
		insNull = new Update(null, tNull);

	}

	@Override
	@AfterMethod
	protected void tearDown() throws Exception {
		super.tearDown();
		for (int i = 0; i < numPeers; ++i) {
			dbs.get(i).disconnect();
		}
		/*
		scm.clear(env);
		scm.quit();
		env.close();*/
		
	}

	@org.testng.annotations.Test(groups = BROKEN_TESTNG_GROUP)
	public void testExchangeUpdatesAndStateExposing() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0t1 = new ArrayList<Update>();
		ArrayList<Update> peer0t2 = new ArrayList<Update>();
		peer0t1.add(insN1);
		peer0t2.add(insNull);
		
		ArrayList<Update> peer1t = new ArrayList<Update>();
		peer1t.add(insM4);
		
		TxnPeerID N1tid = dbs.get(0).addTransaction(peer0t1);
		TxnPeerID Nulltid = dbs.get(0).addTransaction(peer0t2);
		TxnPeerID M4tid = dbs.get(1).addTransaction(peer1t);
		dbs.get(0).publish();
		dbs.get(1).publish();
		int db0r1 = dbs.get(0).reconcile();
		int db1r1 = dbs.get(1).reconcile();
		
		assertTrue(dbs.get(0).hasAcceptedTxn(N1tid));
		assertTrue(dbs.get(0).hasAcceptedTxn(Nulltid));
		assertTrue(dbs.get(0).hasAcceptedTxn(M4tid));
		assertTrue(dbs.get(1).hasAcceptedTxn(N1tid));
		assertTrue(dbs.get(1).hasAcceptedTxn(Nulltid));
		assertTrue(dbs.get(1).hasAcceptedTxn(M4tid));
		
		TupleSet state0 = dbs.get(0).getState().get("R");
		TupleSet state1 = dbs.get(1).getState().get("R");
		assertEquals(3, state0.size());
		assertEquals(3, state1.size());
		assertTrue(tN1.equals(state0.get(tN1)));
		assertTrue(tN1.equals(state1.get(tN1)));
		assertTrue(tM4.equals(state0.get(tM4)));
		assertTrue(tM4.equals(state1.get(tM4)));
		assertTrue(tNull.equals(state0.get(tNull)));
		assertTrue(tNull.equals(state1.get(tNull)));
		assertTrue(tN1.equals(dbs.get(0).getValueForKey(db0r1, tN1)));
		assertTrue(tN1.equals(dbs.get(1).getValueForKey(db1r1, tN1)));
		assertTrue(tM4.equals(dbs.get(0).getValueForKey(db0r1, tM4)));
		assertTrue(tM4.equals(dbs.get(1).getValueForKey(db1r1, tM4)));
		assertTrue(tNull.equals(dbs.get(0).getValueForKey(db0r1, tNull)));
		assertTrue(tNull.equals(dbs.get(1).getValueForKey(db0r1, tNull)));
		
		ResultIterator<ReconciliationEpoch> db0recs = dbs.get(0).getReconciliations();
		assertTrue("Missing element from reconciliations iterator", db0recs.hasNext());
		ReconciliationEpoch re = db0recs.next();
		assertNotNull("Don't expect null element in reconciliations iterator", re);
		
		assertEquals("Incorrect result from reconciliations iterator", StateStore.FIRST_RECNO, re.recno);
		assertFalse("Too many elements in reconciliations iterator", db0recs.hasNext());
		db0recs.close();
		
		HashSet<Decision> decisions = new HashSet<Decision>();
		decisions.add(new Decision(M4tid, 0, true));
		decisions.add(new Decision(N1tid, 0, true));
		decisions.add(new Decision(Nulltid, 0, true));
		ResultIterator<Decision> db1decs = dbs.get(1).getDecisions();
		assertTrue("Not enough elements in decisions iterator", db1decs.hasNext());
		assertFalse("Shouldn't be able to scroll back from start of decisions iterator", db1decs.hasPrev());
		Decision d0 = db1decs.next();
		assertTrue("Incorrect result from decisions iterator", decisions.contains(d0));
		Decision d1 = db1decs.next();
		assertTrue("Incorrect result from decisions iterator", decisions.contains(d1));
		Decision d2 = db1decs.next();
		assertTrue("Incorrect result from decisions iterator", decisions.contains(d2));
		assertFalse("Too many results from decisions iterator", db1decs.hasNext());
		assertTrue("Cannot scroll backwards in decisions iterator", db1decs.hasPrev());
		Decision d22 = db1decs.prev();
		assertEquals("Inconsistent results from decisions iterator", d2, d22);
		assertTrue("Should be able to continue forward in decisions iterator", db1decs.hasNext());
		assertTrue("Should be able to continue back in decisions iterator", db1decs.hasPrev());
		Decision d11 = db1decs.prev();
		assertEquals("Inconsistent results from decisions iterator", d1, d11);
		assertTrue("Should be able to continue forward in decisions iterator", db1decs.hasNext());
		assertTrue("Should be able to continue back in decisions iterator", db1decs.hasPrev());
		Decision d00 = db1decs.prev();
		assertEquals("Inconsistent results from decisions iterator", d0, d00);
		assertTrue("Should be able to continue forward in decisions iterator", db1decs.hasNext());
		assertFalse("Shouldn't be able to scroll back from start of decisions iterator", db1decs.hasPrev());
		db1decs.close();
		
		ResultIterator<Update> updates = dbs.get(0).getPublishedUpdatesForRelation("R");
		assertTrue("Not enough updates in update log", updates.hasNext());
		Update u1 = updates.next(), u2 = updates.next();
		assertTrue("Incorrect first transaction from update log", (u1.equalsOnValues(insN1) && u2.equalsOnValues(insNull)) || (u2.equalsOnValues(insN1) && u1.equalsOnValues(insNull)));
		assertEquals("Incorrect tid from update log", Collections.singleton(N1tid), u1.getTids());
		assertEquals("Incorrect tid from update log", Collections.singleton(Nulltid), u2.getTids());

		//https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=125
		Update u3 = updates.next();
		assertFalse("Too many updates in update log", updates.hasNext());
		assertTrue("Incorrect second transaction from update log", u3.equalsOnValues(insM4));
		assertEquals("Incorrect tid from update log", Collections.singleton(M4tid), u3.getTids());
		updates.close();
		
		List<Update> txn = dbs.get(0).getTransaction(N1tid);
		assertEquals("Incorrect size of first transaction", 1, txn.size());
		assertEquals("Incorrect tid in first transaction", N1tid, txn.get(0).getLastTid());
		assertTrue("Incorrect retrieval of first transaction", txn.get(0).equalsOnValues(insN1));
		
		List<Update> txn2 = dbs.get(0).getTransaction(Nulltid);
		assertEquals("Incorrect size of second transaction", 1, txn2.size());
		assertEquals("Incorrect tid in second transaction", Nulltid, txn2.get(0).getLastTid());
		assertTrue("Incorrect retrieval of second transaction", txn2.get(0).equalsOnValues(insNull));
		
		List<Update> txn3 = dbs.get(1).getTransaction(M4tid);
		assertEquals("Incorrect size of third transaction", 1, txn3.size());
		assertEquals("Incorrect tid in third transaction", M4tid, txn3.get(0).getLastTid());
		assertTrue("Incorrect retrieval of third transaction", txn3.get(0).equalsOnValues(insM4));
	}

	public void testUpdatePropogation() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N3);
		
		TxnPeerID p0 = dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(0).reconcile();
		TxnPeerID p1 = dbs.get(1).addTransaction(peer1);
		dbs.get(1).publish();
		dbs.get(0).reconcile();
		
		assertTrue(dbs.get(1).hasAcceptedTxn(p0));
		assertTrue(dbs.get(1).hasAcceptedTxn(p1));
		assertTrue(dbs.get(0).hasAcceptedTxn(p0));
		assertTrue(dbs.get(0).hasAcceptedTxn(p1));
	}
	
	public void testRejection() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0a = new ArrayList<Update>();
		peer0a.add(insN1);
		ArrayList<Update> peer0b = new ArrayList<Update>();
		peer0b.add(modN1N3);
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N2);
		TxnPeerID p0a = dbs.get(0).addTransaction(peer0a);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		TxnPeerID p0b = dbs.get(0).addTransaction(peer0b);
		TxnPeerID p1 = dbs.get(1).addTransaction(peer1);
		dbs.get(0).publish();
		dbs.get(1).publish();
		dbs.get(0).reconcile();
		dbs.get(1).reconcile();
		dbs.get(0).getCurrentRecno();
		dbs.get(1).getCurrentRecno();
				
		assertTrue(dbs.get(0).hasRejectedTxn(p1));
		assertTrue(dbs.get(1).hasRejectedTxn(p0b));
		assertTrue(dbs.get(0).hasAcceptedTxn(p0a));
		assertTrue(dbs.get(1).hasAcceptedTxn(p0a));
	}
	
	public void testDoesNotConflictWithState() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		dbs.get(0).reconcile();
		dbs.get(1).reconcile();
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N3);
		TxnPeerID peer1tid = dbs.get(1).addTransaction(peer1);
		dbs.get(1).publish();
		dbs.get(0).reconcile();
		
		assertTrue(dbs.get(0).hasAcceptedTxn(peer1tid));
	}
	
	public void testConflictsWithStateRejection() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(insN2);

		TxnPeerID tid0 = dbs.get(0).addTransaction(peer0);
		TxnPeerID tid1 = dbs.get(1).addTransaction(peer1);

		dbs.get(0).publish();
		dbs.get(1).publish();
		dbs.get(0).reconcile();
		dbs.get(1).reconcile();
		
		assertTrue(dbs.get(0).hasRejectedTxn(tid1));
		assertTrue(dbs.get(1).hasRejectedTxn(tid0));
	}
	
	public void testDeferral() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N2);
		ArrayList<Update> peer2 = new ArrayList<Update>();
		peer2.add(modN1N3);
		ArrayList<Update> peer3 = new ArrayList<Update>();
		peer3.add(modN1N4);
		
		assertEquals("Incorrect reconciliation number before first reconciliation", StateStore.FIRST_RECNO, dbs.get(0).getCurrentRecno());

		TxnPeerID tid0 = dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		int r0a = dbs.get(0).reconcile();
		assertEquals("Incorrect reconciliation number for first reconciliation", StateStore.FIRST_RECNO, r0a);
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		TxnPeerID tid1 = dbs.get(1).addTransaction(peer1);
		TxnPeerID tid2 = dbs.get(2).addTransaction(peer2);
		TxnPeerID tid3 = dbs.get(3).addTransaction(peer3);
		
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		int r0b = dbs.get(0).reconcile();
		
		assertEquals("Incorrect reconciliation number for second reconciliation", StateStore.FIRST_RECNO + 1, r0b);

		assertEquals("Incorrect reconciliation number after second reconciliation", StateStore.FIRST_RECNO + 2, dbs.get(0).getCurrentRecno());
		
		assertTrue(dbs.get(1).hasAcceptedTxn(tid0));
		assertTrue(dbs.get(2).hasAcceptedTxn(tid0));
		assertTrue(dbs.get(3).hasAcceptedTxn(tid0));
		// Because they conflict
		assertTrue(dbs.get(1).hasRejectedTxn(tid2));
		assertTrue(dbs.get(1).hasRejectedTxn(tid3));
		assertTrue(dbs.get(2).hasRejectedTxn(tid1));
		assertTrue(dbs.get(2).hasRejectedTxn(tid3));
		assertTrue(dbs.get(3).hasRejectedTxn(tid1));
		assertTrue(dbs.get(3).hasRejectedTxn(tid2));
		// Because they are deferred
		assertFalse(dbs.get(0).hasAcceptedTxn(tid1));
		assertFalse(dbs.get(0).hasRejectedTxn(tid1));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid2));
		assertFalse(dbs.get(0).hasRejectedTxn(tid2));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid3));
		assertFalse(dbs.get(0).hasRejectedTxn(tid3));
		
		Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = dbs.get(0).getConflicts();
		List<List<Set<TxnPeerID>>> conflictsForRecno = conflicts.get(r0b);
		
		assertNotNull("No conflicts for peer 0, recno r0b", conflictsForRecno);
		assertEquals("Wrong number of conflicts retrieved", 1, conflictsForRecno.size());
		List<Set<TxnPeerID>> options = conflictsForRecno.get(0);
		assertEquals("Wrong number of conflict options retrieved", 3, options.size());
		assertTrue("Missing transaction ID for peer 1 in conflicts",
			(options.get(0).contains(tid1) || options.get(1).contains(tid1) || options.get(2).contains(tid1)));
		assertTrue("Missing transaction ID for peer 2 in conflicts",
			(options.get(0).contains(tid2) || options.get(1).contains(tid2) || options.get(2).contains(tid2)));
		assertTrue("Missing transaction ID for peer 3 in conflicts",
				(options.get(0).contains(tid3) || options.get(1).contains(tid3) || options.get(2).contains(tid3)));
		assertEquals("Extra transaction ID in option 0", 1, options.get(0).size());
		assertEquals("Extra transaction ID in option 1", 1, options.get(1).size());
		assertEquals("Extra transaction ID in option 2", 1, options.get(2).size());

		List<Update> tid1Ret = dbs.get(0).getTransaction(tid1);
		assertEquals("Wrong number of updates in tid1", 1, tid1Ret.size());
		assertEquals("Missing antecedent from tid1", Collections.singleton(tid0), tid1Ret.get(0).getPrevTids());
	}
	
	public void testResolveConflictsRejectAll() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N2);
		ArrayList<Update> peer2 = new ArrayList<Update>();
		peer2.add(modN1N3);
		ArrayList<Update> peer3 = new ArrayList<Update>();
		peer3.add(modN1N4);
		
		dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		TxnPeerID tid1 = dbs.get(1).addTransaction(peer1);
		TxnPeerID tid2 = dbs.get(2).addTransaction(peer2);
		TxnPeerID tid3 = dbs.get(3).addTransaction(peer3);
		
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		int r0b = dbs.get(0).reconcile();
		
		HashMap<Integer,Integer> resolution = new HashMap<Integer,Integer>();
		resolution.put(0,null);
		HashMap<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
		resolutions.put(r0b,resolution);
		dbs.get(0).resolveConflicts(resolutions);

		assertFalse(dbs.get(0).hasAcceptedTxn(tid1));
		assertTrue(dbs.get(0).hasRejectedTxn(tid1));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid2));
		assertTrue(dbs.get(0).hasRejectedTxn(tid2));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid3));
		assertTrue(dbs.get(0).hasRejectedTxn(tid3));
	}

	public void testResolveConflicts() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		ArrayList<Update> peer1 = new ArrayList<Update>();
		peer1.add(modN1N2);
		ArrayList<Update> peer2 = new ArrayList<Update>();
		peer2.add(modN1N3);
		ArrayList<Update> peer3 = new ArrayList<Update>();
		peer3.add(modN1N4);
		
		dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		dbs.get(0).reconcile();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		Set<TxnPeerID> allTids = new HashSet<TxnPeerID>();
		allTids.add(dbs.get(1).addTransaction(peer1));
		allTids.add(dbs.get(2).addTransaction(peer2));
		allTids.add(dbs.get(3).addTransaction(peer3));
		
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		dbs.get(3).reconcile();
		int r0b = dbs.get(0).reconcile();
		
		Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = dbs.get(0).getConflicts();
		List<List<Set<TxnPeerID>>> conflictsForRecno = conflicts.get(r0b);
		Set<TxnPeerID> acceptedTids = conflictsForRecno.get(0).get(0); 

		HashMap<Integer,Integer> resolution = new HashMap<Integer,Integer>();
		resolution.put(0,0);
		HashMap<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
		resolutions.put(r0b,resolution);
		dbs.get(0).resolveConflicts(resolutions);

		for (TxnPeerID tpi : acceptedTids) {		
			assertTrue(dbs.get(0).hasAcceptedTxn(tpi));
			assertFalse(dbs.get(0).hasRejectedTxn(tpi));
		}
		for (TxnPeerID tpi : allTids) {
			if (! acceptedTids.contains(tpi)) {
				assertFalse(dbs.get(0).hasAcceptedTxn(tpi));
				assertTrue(dbs.get(0).hasRejectedTxn(tpi));
			}
		}
	}
	
	@org.testng.annotations.Test(groups = BROKEN_TESTNG_GROUP)
	public void testDirtyValues() throws Exception {
		setAllTrusted();
		ArrayList<Update> peer0 = new ArrayList<Update>();
		peer0.add(insN1);
		ArrayList<Update> peer1a = new ArrayList<Update>();
		peer1a.add(modN1N2);
		ArrayList<Update> peer1b = new ArrayList<Update>();
		peer1b.add(modN2N5);
		ArrayList<Update> peer2 = new ArrayList<Update>();
		peer2.add(modN1N3);
		
		dbs.get(0).addTransaction(peer0);
		dbs.get(0).publish();
		dbs.get(0).reconcile();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();

		TxnPeerID tid1a = dbs.get(1).addTransaction(peer1a);
		TxnPeerID tid2 = dbs.get(2).addTransaction(peer2);

		dbs.get(1).publish();
		dbs.get(2).publish();
		
		int r0b = dbs.get(0).reconcile();
		TxnPeerID tid1b = dbs.get(1).addTransaction(peer1b);
		dbs.get(1).publish();
		dbs.get(0).reconcile();
		
		// All should be deferred
		// https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=124
		assertFalse(dbs.get(0).hasRejectedTxn(tid1a));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid1a));
		assertFalse(dbs.get(0).hasRejectedTxn(tid2));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid2));
		assertFalse(dbs.get(0).hasRejectedTxn(tid1b));
		assertFalse(dbs.get(0).hasAcceptedTxn(tid1b));

		Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = dbs.get(0).getConflicts();

		int option = -1;
		List<Set<TxnPeerID>> options = conflicts.get(r0b).get(0); 
		int numOptions = options.size();
		for (int i = 0; i < numOptions; ++i) {
			if (options.get(i).contains(tid1a)) {
				option = i;
			}
		}
		
		HashMap<Integer,Integer> resolution = new HashMap<Integer,Integer>();
		resolution.put(0,option);
		HashMap<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
		resolutions.put(r0b,resolution);
		dbs.get(0).resolveConflicts(resolutions);
		
		assertTrue(dbs.get(0).hasAcceptedTxn(tid1a));
		assertTrue(dbs.get(0).hasRejectedTxn(tid2));
		assertTrue(dbs.get(0).hasAcceptedTxn(tid1b));
	}

	public void testSubsumption() throws Exception {
		setSomeTrusted();
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		
		t1.add(insN1);
		t2.add(modN1N3);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		TxnPeerID tpi2 = dbs.get(0).addTransaction(t2);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		
		assertTrue(dbs.get(1).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(1).hasAcceptedTxn(tpi2));
	}
	
	
	public void testOverlappingAntecedents() throws Exception {
		setSomeTrusted();
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();
		
		t1.add(insN1);
		t1.add(insM4);
		
		t2.add(modN1N3);
		
		t3.add(modM4M5);

		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		assertTrue(dbs.get(1).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(2).hasAcceptedTxn(tpi1));
		TxnPeerID tpi2 = dbs.get(1).addTransaction(t2);
		TxnPeerID tpi3 = dbs.get(2).addTransaction(t3);
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).reconcile();

		assertTrue(dbs.get(3).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi2));
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi3));
	}
	
	public void testMultipleUse() throws Exception {
		setSomeTrusted();
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();

		t1.add(insN1);
		t2.add(modN1N3);
		t3.add(modN1M5);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		TxnPeerID tpi2 = dbs.get(1).addTransaction(t2);
		TxnPeerID tpi3 = dbs.get(2).addTransaction(t3);
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).reconcile();
		
		// All should be deferred
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi1));
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi2));
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi3));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi1));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi2));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi3));	
	}
	
	public void testMultipleUse2() throws Exception {
		setSomeTrusted();
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();

		t1.add(insN1);
		t2.add(delN1);
		t3.add(modN1M5);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		TxnPeerID tpi2 = dbs.get(1).addTransaction(t2);
		TxnPeerID tpi3 = dbs.get(2).addTransaction(t3);
		dbs.get(1).publish();
		dbs.get(2).publish();
		dbs.get(3).reconcile();
		
		// All should be deferred
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi1));
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi2));
		assertFalse(dbs.get(3).hasAcceptedTxn(tpi3));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi1));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi2));
		assertFalse(dbs.get(3).hasRejectedTxn(tpi3));	
	}
	
	public void testMultiwayConflict() throws Exception {
		setSomeTrusted();
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();
		
		t1.add(insN1);
		t2.add(insN2);
		t3.add(insN3);
		
		dbs.get(0).addTransaction(t1);
		dbs.get(1).addTransaction(t2);
		dbs.get(2).addTransaction(t3);
		dbs.get(0).publish();
		dbs.get(1).publish();
		dbs.get(2).publish();
		int db4r1 = dbs.get(4).reconcile();

		Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = dbs.get(4).getConflicts();
		// There should only be one recno
		assertEquals(1, conflicts.size());
		List<List<Set<TxnPeerID>>> conflictsForRecno = conflicts.get(db4r1);
		// There should only be one conflict
		assertEquals(1, conflictsForRecno.size());
		// And it should have three options
		assertEquals(3, conflictsForRecno.get(0).size());
	}
	
	public void testModifiedAntecedent() throws Exception {
		setAllTrusted();

		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();

		t1.add(insN1);
		t2.add(modN1N2);
		t3.add(modN1N3);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		TxnPeerID tpi2 = dbs.get(1).addTransaction(t2);
		dbs.get(1).publish();
		dbs.get(3).reconcile();
		
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi2));
		
		TxnPeerID tpi3 = dbs.get(2).addTransaction(t3);
		dbs.get(2).publish();
		dbs.get(3).reconcile();
		
		assertTrue(dbs.get(3).hasRejectedTxn(tpi3));
	}
	
	
	public void testOldAndNewValuesInState() throws Exception {
		setAllTrusted();
		
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();

		t1.add(insN1);
		t2.add(modN1M5);
		t3.add(insM5);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		TxnPeerID tpi2 = dbs.get(0).addTransaction(t2);
		dbs.get(0).reconcile();
		TxnPeerID tpi3 = dbs.get(1).addTransaction(t3);
		dbs.get(1).publish();
		
		dbs.get(0).reconcile();
		
		assertTrue(dbs.get(0).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(0).hasAcceptedTxn(tpi2));
		assertTrue(dbs.get(0).hasAcceptedTxn(tpi3));
		
		Tuple st = dbs.get(0).getValueForKey(dbs.get(0).getCurrentRecno(), tN1); 
		assertNull(st);
		
	}
	
	public void testInsertionsAndDeletions() throws Exception {
		setAllTrusted();
		
		ArrayList<Update> t1 = new ArrayList<Update>();
		ArrayList<Update> t2 = new ArrayList<Update>();
		ArrayList<Update> t3 = new ArrayList<Update>();
		
		t1.add(insN1);
		t1.add(insM5);
		
		TxnPeerID tpi1 = dbs.get(0).addTransaction(t1);
		dbs.get(0).publish();
		
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		
		t2.add(delN1);
		TxnPeerID tpi2 = dbs.get(1).addTransaction(t2);
		dbs.get(1).publish();
		
		t3.add(delM5);
		TxnPeerID tpi3 = dbs.get(2).addTransaction(t3);
		dbs.get(2).publish();
		
		int db3r0 = dbs.get(3).reconcile();
		
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi1));
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi2));
		assertTrue(dbs.get(3).hasAcceptedTxn(tpi3));

		Tuple vN = dbs.get(3).getValueForKey(db3r0, tN1);
		assertNull("R(N,1) should have been deleted", vN);
		Tuple vM = dbs.get(3).getValueForKey(db3r0, tM5); 
		assertNull("R(M,5) should have neen deleted", vM);
	}
	
	public void testReconciliationTxns() throws Exception {
		setAllTrusted();
		
		ArrayList<Update> t1 = new ArrayList<Update>();
		t1.add(insM4);
		ArrayList<Update> t2 = new ArrayList<Update>();
		t2.add(modM4M5);
		ArrayList<Update> t3 = new ArrayList<Update>();
		t3.add(insN1);
		ArrayList<Update> t4 = new ArrayList<Update>();
		t4.add(delM5);
		
		Db p0 = dbs.get(0);
		Db p4 = dbs.get(4);
		
		TxnPeerID tid1 = p0.addTransaction(t1);
		p0.publish();
		
		// Haven't reconciled yet
		ResultIterator<TxnPeerID> ri1 = p4.getTransactionsForReconciliation(p4.getCurrentRecno());
		assertTrue("Not enough txns in iterator", ri1.hasNext());
		assertEquals("Incorrect result from iterator", tid1, ri1.next());
		assertFalse("Too many results in iterator", ri1.hasNext());
		ri1.close();
		
		int r0 = p4.reconcile();
		
		// Retrieve first txn
		ResultIterator<TxnPeerID> ri2 = p4.getTransactionsForReconciliation(r0);
		assertTrue("Not enough txns in iterator", ri2.hasNext());
		assertEquals("Incorrect result from iterator", tid1, ri2.next());
		assertFalse("Too many results in iterator", ri2.hasNext());
		ri2.close();
		
		TxnPeerID tid2 = p0.addTransaction(t2);
		p0.publish();
		TxnPeerID tid3 = p4.addTransaction(t3);
		p4.publish();
		
		// Retrieve tids since last reconcilation
		ResultIterator<TxnPeerID> ri3 = p4.getTransactionsForReconciliation(p4.getCurrentRecno());
		Set<TxnPeerID> tids = new HashSet<TxnPeerID>(), expected = new HashSet<TxnPeerID>();
		assertTrue("Not enough txns in iterator", ri3.hasNext());
		tids.add(ri3.next());
		assertTrue("Not enough txns in iterator", ri3.hasNext());
		tids.add(ri3.next());
		assertFalse("Too many results in iterator", ri3.hasNext());
		ri3.close();
		expected.add(tid2);
		expected.add(tid3);
		assertEquals("Incorrect results from iterator", expected, tids);
		
		int r1 = p4.reconcile();
		
		// Retrieve tids for an intermediate reconciliation
		tids.clear();
		ResultIterator<TxnPeerID> ri4 = p4.getTransactionsForReconciliation(r1);
		assertTrue("Not enough txns in iterator", ri4.hasNext());
		tids.add(ri4.next());
		assertTrue("Not enough txns in iterator", ri4.hasNext());
		tids.add(ri4.next());
		assertFalse("Too many results in iterator", ri4.hasNext());
		ri4.close();

		assertEquals("Incorrect results from iterator", expected, tids);
		
	}
	
	@org.testng.annotations.Test(groups = BROKEN_TESTNG_GROUP)
	public void testSimpleReplay() throws Exception {
		setAllTrusted();
		List<Update> p1t = new ArrayList<Update>();
		p1t.add(insN1);
		List<Update> p2t = new ArrayList<Update>();
		p2t.add(insN2);
		List<Update> p3t = new ArrayList<Update>();
		p3t.add(insM4);
		
		TxnPeerID tidN1 = dbs.get(1).addTransaction(p1t);
		dbs.get(1).publish();
		TxnPeerID tidN2 = dbs.get(2).addTransaction(p2t);
		dbs.get(2).publish();
		TxnPeerID tidM4 = dbs.get(3).addTransaction(p3t);
		dbs.get(3).publish();
		
		int recno = dbs.get(0).reconcile();
		
		assertTrue(dbs.get(0).hasAcceptedTxn(tidM4));
		assertFalse(dbs.get(0).hasAcceptedTxn(tidN1));
		assertFalse(dbs.get(0).hasRejectedTxn(tidN1));
		assertFalse(dbs.get(0).hasAcceptedTxn(tidN2));
		assertFalse(dbs.get(0).hasRejectedTxn(tidN2));
		List<Set<TxnPeerID>> conflicts = dbs.get(0).getConflicts().get(recno).get(0);
		assertEquals("Wrong number of options", 2, conflicts.size());
		assertTrue("Wrong options", (conflicts.get(0).equals(Collections.singleton(tidN1)) && conflicts.get(1).equals(Collections.singleton(tidN2)))
				|| (conflicts.get(1).equals(Collections.singleton(tidN1)) && conflicts.get(0).equals(Collections.singleton(tidN2))));
		
		ClientCentricDb newDb = new ClientCentricDb(sys, scm, s, new StringPeerID("p"), tcs.get(0), factory, HashTableStore.FACTORY);
		
		assertEquals("Differing state after replaying previous reconciliation", dbs.get(0).getState(), newDb.getState());
		
		List<Set<TxnPeerID>> newConflicts = newDb.getConflicts().get(recno).get(0);
		
		assertEquals("Different number of conflicts after replaying reconciliation", conflicts.size(), newConflicts.size());
		assertEquals("Wrong number of options after replaying reconciliation", 2, conflicts.size());
		assertTrue("Wrong options after replaying reconciliation", (conflicts.get(0).equals(Collections.singleton(tidN1)) && conflicts.get(1).equals(Collections.singleton(tidN2)))
				|| (conflicts.get(1).equals(Collections.singleton(tidN1)) && conflicts.get(0).equals(Collections.singleton(tidN2))));

		newDb.disconnect();
		
		Map<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
		Map<Integer,Integer> recRes = new HashMap<Integer,Integer>();
		resolutions.put(recno, recRes);
		if (conflicts.get(0).contains(tidN1)) {
			recRes.put(0, 0);
		} else {
			recRes.put(0, 1);
		}
		
		dbs.get(0).resolveConflicts(resolutions);
		assertTrue("Conflict resolution did not succeed", dbs.get(0).hasAcceptedTxn(tidN1));
		assertTrue("Conflict resolution did not succeed", dbs.get(0).hasRejectedTxn(tidN2));

		newDb = new ClientCentricDb(sys, scm, s, new StringPeerID("p"), tcs.get(0), factory, HashTableStore.FACTORY);
		assertEquals("Differing state after replaying previous reconciliation and conflict resolution", dbs.get(0).getState(), newDb.getState());
		newDb.disconnect();
		
		newDb = new ClientCentricDb(sys, scm, s, new StringPeerID("q"), tcs.get(3), factory, HashTableStore.FACTORY);		
		
		List<Update> p3t2 = Collections.singletonList(insNull);
		TxnPeerID tidNull = newDb.addTransaction(p3t2);
		assertTrue("Bad TID for transaction after replay", tidNull.getTid() > tidM4.getTid());
		newDb.publish();
}
	@org.testng.annotations.Test(groups = BROKEN_TESTNG_GROUP)
	public void testComplexReplay() throws Exception {
		setSomeTrusted();
		
		List<Update> p0t = Collections.singletonList(insN1);
		List<Update> p1t = Collections.singletonList(modN1N2);
		List<Update> p2t = Collections.singletonList(modN1N3);
		
		TxnPeerID p0tid = dbs.get(0).addTransaction(p0t);
		dbs.get(0).publish();
		dbs.get(1).reconcile();
		dbs.get(2).reconcile();
		
		assertTrue("Peer 1 should have accepted +(N,1)", dbs.get(1).hasAcceptedTxn(p0tid));
		assertTrue("Peer 2 should have accepted +(N,1)", dbs.get(2).hasAcceptedTxn(p0tid));
		
		TxnPeerID p1tid = dbs.get(1).addTransaction(p1t);
		TxnPeerID p2tid = dbs.get(2).addTransaction(p2t);
		
		dbs.get(1).publish();
		dbs.get(2).publish();
		
		int db4r0 = dbs.get(4).reconcile();
		
		assertTrue("Peer 4 should have accepted +(N,1)", dbs.get(4).hasAcceptedTxn(p0tid));
		assertFalse("Peer 4 should not have accepted (N,1)->(N,2)", dbs.get(4).hasAcceptedTxn(p1tid));
		assertFalse("Peer 4 should not have accepted (N,1)->(N,3)", dbs.get(4).hasAcceptedTxn(p2tid));
		assertFalse("Peer 4 should not have rejected (N,1)->(N,2)", dbs.get(4).hasRejectedTxn(p1tid));
		assertFalse("Peer 4 should not have rejected (N,1)->(N,3)", dbs.get(4).hasRejectedTxn(p2tid));
		
		
		ClientCentricDb newDb = new ClientCentricDb(sys, scm, s, new StringPeerID("r"), tcs.get(4), factory, HashTableStore.FACTORY);
		
		assertEquals("Differing state after replaying reconciliation", dbs.get(4).getState(), newDb.getState());
		assertEquals("Wrong number of conflict options after replaying reconciliation", 2, dbs.get(4).getConflicts().get(db4r0).get(0).size());
		
		List<Set<TxnPeerID>> conflicts = dbs.get(4).getConflicts().get(db4r0).get(0);

		Map<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
		Map<Integer,Integer> recRes = new HashMap<Integer,Integer>();
		resolutions.put(db4r0, recRes);
		if (conflicts.get(0).contains(p2tid)) {
			recRes.put(0, 0);
		} else {
			recRes.put(0, 1);
		}
		
		newDb.resolveConflicts(resolutions);
		assertTrue("Peer #4 should have accepted (N,1)->(N,3)", newDb.hasAcceptedTxn(p2tid));
		assertTrue("Peer #4 should have rejected (N,1)->(N,2)", newDb.hasRejectedTxn(p1tid));
		
		ClientCentricDb newDb2 = new ClientCentricDb(sys, scm, s, new StringPeerID("t"), tcs.get(4), factory, HashTableStore.FACTORY);
		
		assertEquals("Differing state after replaying reconciliations", newDb.getState(), newDb2.getState());
	}
	
	
	@org.testng.annotations.Test(groups = BROKEN_TESTNG_GROUP)
	public void testDump() throws Exception {
		setAllTrusted();

		List<Update> p1t = new ArrayList<Update>();
		p1t.add(insN1);
		List<Update> p2t = new ArrayList<Update>();
		p2t.add(insN2);
		List<Update> p3t = new ArrayList<Update>();
		p3t.add(insM4);
		List<Update> p3t2 = new ArrayList<Update>();
		p3t2.add(modM4M5);
		
		
		TxnPeerID tidN1 = dbs.get(1).addTransaction(p1t);
		dbs.get(1).publish();
		TxnPeerID tidN2 = dbs.get(2).addTransaction(p2t);
		dbs.get(2).publish();
		TxnPeerID tidM4 = dbs.get(3).addTransaction(p3t);
		dbs.get(3).publish();
		
		int recno = dbs.get(0).reconcile();
		
		TxnPeerID tidM5 = dbs.get(3).addTransaction(p3t2);
		dbs.get(3).publish();

		int recno2 = dbs.get(0).reconcile();
		
		assertTrue(dbs.get(0).hasAcceptedTxn(tidM4));
		assertFalse(dbs.get(0).hasAcceptedTxn(tidN1));
		assertFalse(dbs.get(0).hasRejectedTxn(tidN1));
		assertFalse(dbs.get(0).hasAcceptedTxn(tidN2));
		assertFalse(dbs.get(0).hasRejectedTxn(tidN2));
		assertTrue(dbs.get(0).hasAcceptedTxn(tidM5));
		
		List<Update> insN1t = dbs.get(0).getTransaction(tidN1);
		List<Update> modM4M5t = dbs.get(0).getTransaction(tidM5);
		
		for (Db db : dbs) {
			db.disconnect();
		}
		
		//https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=127
		USDump dump = factory.dumpUpdateStore(scm, s);

		Set<Decision> decs = new HashSet<Decision>(), decsExpected = new HashSet<Decision>();
		decsExpected.add(new Decision(tidM4, recno, true));
		decsExpected.add(new Decision(tidM5, recno2, true));
		Iterator<Decision> p0decs = dump.getPeerDecisions(peers.get(0));
		while (p0decs.hasNext()) {
			decs.add(p0decs.next());
		}
		assertEquals("Incorrect decisions from dump", decsExpected, decs);
		
		Set<TxnPeerID> dumpTids = new HashSet<TxnPeerID>(), expectedTids = new HashSet<TxnPeerID>();
		expectedTids.add(tidN1);
		expectedTids.add(tidN2);
		expectedTids.add(tidM4);
		expectedTids.add(tidM5);
		
		Iterator<TxnPeerID> tidIt = dump.getTids();
		while (tidIt.hasNext()) {
			dumpTids.add(tidIt.next());
		}
		
		assertEquals("Incorrect TIDs in dump", expectedTids, dumpTids);
		
		assertEquals("Incorrect transaction contents from dump", insN1t, dump.getTxnContents(tidN1));
		assertEquals("Incorrect transaction contents from dump", modM4M5t, dump.getTxnContents(tidM5));
		
		factory.resetStore(s);
		factory.restoreUpdateStore(dump);
		
		setAllTrusted();
		
		List<Set<TxnPeerID>> conflicts = dbs.get(0).getConflicts().get(recno).get(0);
		assertEquals("Wrong number of options", 2, conflicts.size());
		assertTrue("Wrong options", (conflicts.get(0).equals(Collections.singleton(tidN1)) && conflicts.get(1).equals(Collections.singleton(tidN2)))
				|| (conflicts.get(1).equals(Collections.singleton(tidN1)) && conflicts.get(0).equals(Collections.singleton(tidN2))));

	}
	
}
