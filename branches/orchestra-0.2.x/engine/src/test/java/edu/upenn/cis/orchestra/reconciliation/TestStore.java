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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.testng.annotations.BeforeMethod;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP})
abstract public class TestStore extends TestCase {
	Relation r, s, t;
	Schema schema;
	ISchemaIDBinding schMap;
	Tuple tN1, tN2, tN3, tM4, tM5, tN6, tS1, tT1;
	Update insN1, insN2, insN3, modN1N3, insM4, modM4M5, modN1M5, delN1, modN3N6, insS1, insT1;
	List<Update> insN1t, insN2t, delN1t, modN1N3t, modN3N6t, insN3t, insM4t, insS1t, insT1t;
	StateStore ss;
	int r0;
	
	abstract StateStore getStore(AbstractPeerID ipi, ISchemaIDBinding sm, Schema s) throws Exception;
	
	@BeforeMethod
	protected void setUp() throws Exception {
		super.setUp();
		
		schema = new Schema(getClass().getSimpleName() + "_schema");
		s = schema.addRelation("S");
		s.addCol("a", IntType.INT);
		s.setPrimaryKey(new PrimaryKey("pk", s, Collections.singleton("a")));
		r = schema.addRelation("R");
		r.addCol("name", new StringType(true, true, true, 10));
		r.addCol("val", IntType.INT);
		r.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("name")));
		t = schema.addRelation("T");
		t.addCol("b", IntType.INT);
		t.setPrimaryKey(new PrimaryKey("pk", t, Collections.singleton("b")));
		schema.markFinished();
		Peer peer = new Peer("0", "localhost", "TestStore peer");
		AbstractPeerID peerID = peer.getPeerId();
		Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap();
		peerIDToSchema.put(peerID, schema);
		schMap = new LocalSchemaIDBinding(peerIDToSchema);
		ss = getStore(peerID, schMap, schema);

		ss.reset();
		
		tN1 = new Tuple(r);
		tN2 = new Tuple(r);
		tN3 = new Tuple(r);
		tM4 = new Tuple(r);
		tM5 = new Tuple(r);
		tN6 = new Tuple(r);
		tS1 = new Tuple(s);
		tT1 = new Tuple(t);
		tN1.set("name", "Nick");
		tN2.set("name", "Nick");
		tN3.set("name", "Nick");
		tM4.set("name", "Mark");
		tM5.set("name", "Mark");
		tN6.set("name", "Nick");
		tN1.set("val", 1);
		tN2.set("val", 2);
		tN3.set("val", 3);
		tM4.set("val", 4);
		tM5.set("val", 5);
		tN6.set("val", 6);
		tS1.set("a", 1);
		tT1.set("b", 1);
		insN1 = new Update(null, tN1);
		insN2 = new Update(null, tN2);
		insN3 = new Update(null, tN3);
		modN1N3 = new Update(tN1, tN3);
		insM4 = new Update(null, tM4);
		modM4M5 = new Update(tM4, tM5);
		modN1M5 = new Update(tN1, tM5);
		delN1 = new Update(tN1, null);
		modN3N6 = new Update(tN3, tN6);
		insS1 = new Update(null, tS1);
		insT1 = new Update(null, tT1);
		insN1t = new ArrayList<Update>(1);
		insN2t = new ArrayList<Update>(1);
		insN3t = new ArrayList<Update>(1);
		delN1t = new ArrayList<Update>(1);
		modN1N3t = new ArrayList<Update>(1);
		modN3N6t = new ArrayList<Update>(1);
		insM4t = new ArrayList<Update>(1);
		insS1t = new ArrayList<Update>(1);
		insT1t = new ArrayList<Update>(1);
		insN1t.add(insN1);
		insN2t.add(insN2);
		insN3t.add(insN3);
		delN1t.add(delN1);
		modN1N3t.add(modN1N3);
		modN3N6t.add(modN3N6);
		insM4t.add(insM4);
		insS1t.add(insS1);
		insT1t.add(insT1);
		
		r0 = ss.getCurrentRecno();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testPrimaryInsertion() throws Exception {
		
		TxnPeerID tpi = ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0,insN1t);
		assertEquals(tpi,insN1.getLastTid());
		assertTrue(ss.containsTuple(r0,tN1));
	}

	public void testPrimaryUpdate() throws Exception {
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0,insN1t);
		ss.prepareTransaction(modN1N3t);
		ss.applyTransaction(r0,modN1N3t);
		assertTrue(ss.containsTuple(r0,tN3));
		assertFalse(ss.containsTuple(r0,tN1));
	}

	public void testPrimaryDelete() throws Exception {
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0,insN1t);
		ss.prepareTransaction(delN1t);
		ss.applyTransaction(r0,delN1t);
		assertFalse(ss.containsTuple(r0,tN1));
	}
	
	public void testAdvanceRecno() throws Exception {
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r1,insN1t);
		assertTrue(ss.containsTuple(r1,tN1));
		assertFalse(ss.containsTuple(r0,tN1));
	}

	public void testMultipleRecnos() throws Exception {
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0,insN1t);
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		ss.prepareTransaction(modN1N3t);
		ss.applyTransaction(r1,modN1N3t);
		ss.advanceRecno();
		int r2 = ss.getCurrentRecno();
		ss.prepareTransaction(modN3N6t);
		ss.applyTransaction(r2,modN3N6t);
		assertTrue(ss.containsTuple(r0,tN1));
		assertFalse(ss.containsTuple(r1,tN1));
		assertFalse(ss.containsTuple(r2,tN1));
		assertTrue(ss.containsTuple(r1,tN3));
		assertFalse(ss.containsTuple(r0,tN3));
		assertFalse(ss.containsTuple(r2,tN3));
		assertTrue(ss.containsTuple(r2,tN6));
		assertFalse(ss.containsTuple(r0,tN6));
		assertFalse(ss.containsTuple(r1,tN6));
	}

	public void testSecondaryInsertion() throws Exception {
		insN1.addTid(0, new IntPeerID(1));
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		ss.advanceRecno();
		int r2 = ss.getCurrentRecno();
		ss.applyTransaction(r1,insN1t);
		assertTrue(ss.containsTuple(r1,tN1));
		assertTrue(ss.containsTuple(r2,tN1));
		assertFalse(ss.containsTuple(r0,tN1));
	}
	
	public void testInsertError() throws Exception {
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0,insN1t);
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		boolean caughtError = false;
		ss.prepareTransaction(insN2t);
		try {
			ss.applyTransaction(r1,insN2t);
		} catch (StateStore.SSException sse) {
			caughtError = true;
		}
		assertTrue(caughtError);
	}
		
	public void testDirtyModify() throws Exception {
		boolean caughtError = false;
		
		ss.prepareTransaction(insN1t);
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		ss.prepareTransaction(insN2t);
		ss.applyTransaction(r1, insN2t);
		
		try {
			ss.applyTransaction(r0, insN1t);
		} catch (StateStore.SSException sse) {
			caughtError = true;
		}
		assertTrue(caughtError);
	}
	
	public void testMultipleUpdates() throws Exception {
		ss.prepareTransaction(insN1t);
		ss.applyTransaction(r0, insN1t);
		ss.prepareTransaction(modN1N3t);
		ss.applyTransaction(r0, modN1N3t);
		ss.prepareTransaction(modN3N6t);
		ss.applyTransaction(r0, modN3N6t);
		ss.advanceRecno();
		int r1 = ss.getCurrentRecno();
		
		assertTrue(ss.containsTuple(r0, tN6));
		assertTrue(ss.containsTuple(r1, tN6));
		
	}
	
	public void testMultipleAntecedents() throws Exception {
		List<Update> insN3t2 = new ArrayList<Update>();
		insN3t2.add(insN3.duplicate());
		ss.prepareTransaction(insN3t);
		ss.applyTransaction(r0, insN3t);
		ss.prepareTransaction(insN3t2);
		ss.applyTransaction(r0, insN3t2);
		ss.prepareTransaction(modN3N6t);
		assertEquals(2,modN3N6.getPrevTids().size());
		assertTrue(modN3N6.isPrevTid(insN3.getLastTid()));
		assertTrue(modN3N6.isPrevTid(insN3t2.get(0).getLastTid()));
	}
	
	public void testStateIterator() throws Exception {
		ss.prepareTransaction(insN3t);
		ss.prepareTransaction(insM4t);
		ss.prepareTransaction(insS1t);
		ss.prepareTransaction(insT1t);
		ss.applyTransaction(r0, insN3t);
		ss.applyTransaction(r0, insM4t);
		ss.applyTransaction(r0, insS1t);
		ss.applyTransaction(r0, insT1t);
		
		
		ResultIterator<Tuple> ri = ss.getStateIterator("R");
		TupleSet ts = new TupleSet();
		
		assertTrue("Not enough elements in result iterator", ri.hasNext());
		Tuple t1 = ri.next();
		t1.setReadOnly();
		ts.add(t1);
		
		assertTrue("Not enough elements in result iterator", ri.hasNext());
		Tuple t2 = ri.next();
		t2.setReadOnly();
		ts.add(t2);
		
		assertFalse("Too many elements in result iterator", ri.hasNext());
		
		assertEquals("Reverse iteration does not work correctly", t2, ri.prev());
		assertEquals("Reverse iteration does not work correctly", t1, ri.prev());
		assertFalse("Too many elements in reverse iterator", ri.hasPrev());
		
		ri.close();
	}
	
}
