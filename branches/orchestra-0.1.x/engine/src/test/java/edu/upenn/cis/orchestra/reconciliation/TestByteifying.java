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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.After;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.LongType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringPeerID;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.Predicate;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP})
public class TestByteifying extends TestCase {
	

	Schema s;
	ISchemaIDBinding scm;
	Relation rs;
	Tuple tN1;
	Tuple tM4;
	Tuple tH;
	Tuple ln;
	IntPeerID ipi1;
	IntPeerID ipi1234567890;
	StringPeerID spiHello, spiForeign;
	TxnPeerID tpi1;
	TxnPeerID tpi2;
	TxnPeerID tpi3;
	Update u;
	Update ins;
	Update del;
	TrustConditions tc;
	
	@Override
	@BeforeMethod
	protected void setUp() throws Exception {
		super.setUp();

		s = new Schema(getClass().getSimpleName() + "_schema");
		s.setDescription("");
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(true, false, true, 50));
		rs.addCol("val", new IntType(true, false));
		rs.addCol("val2", new DoubleType(true, false));
		rs.addCol("val3", new IntType(true, false));
		s.markFinished();
		
		List<Schema> schemas = new ArrayList<Schema>();
		schemas.add(s);
		Map<AbstractPeerID,Schema> peerIDToSchema = new HashMap<AbstractPeerID,Schema>();
		peerIDToSchema.put(new StringPeerID("a"), s);
		scm = new LocalSchemaIDBinding(peerIDToSchema);

		tN1 = new Tuple(rs);
		tN1.set(0, "Nick");
		tN1.set(1, 1);
		tN1.set(2, -0.4563465);
		tM4 = new Tuple(rs);
		tM4.set(0, "Mark");
		tM4.set(1, 4);
		tH = new Tuple(rs);
		tH.set(0, "¡Hello! «Non so più cosa son, cosa faccio!»");
		ln = new Tuple(rs);
		ln.setLabeledNull("name", -55);
		ln.setLabeledNull(1, 55);
		ln.set(2, 3.14159);
		ipi1 = new IntPeerID(1);
		ipi1234567890 = new IntPeerID(1234567890);
		tpi1 = new TxnPeerID(5432, ipi1);
		tpi2 = new TxnPeerID(12345678,ipi1234567890);
		spiHello = new StringPeerID("Hello!");
		spiForeign = new StringPeerID("¡Hello! «Non so più cosa son, cosa faccio!»");
		u = new Update(tN1, tM4);
		u.addTid(tpi2);
		u.addTid(tpi1);
		tpi3 = new TxnPeerID(5, new IntPeerID(3));
		u.addPrevTid(tpi3);
		u.setChangedFromTo(tH);
		ins = new Update(null, tN1);
		del = new Update(tN1, null);
		
		tc = new TrustConditions(ipi1);
		Predicate p = new AndPred(ComparePredicate.createTwoCols(rs, "val",ComparePredicate.Op.NE,"val3"), ComparePredicate.createColLit(rs, "name", ComparePredicate.Op.EQ, "Nick"));
		tc.addTrustCondition(ipi1234567890, rs.getRelationID(), p, 10);
	}
	
	public void testInt() {
		int testValue = 1;
		byte[] testBytes = IntType.getBytes(testValue);
		// All values stored MSB-first
		assertEquals(1, testBytes[3]);
		int test = IntType.getValFromBytes(testBytes, 0);
		assertEquals(testValue, test);
		
		int testValue2 = 256;
		byte[] testBytes2 = IntType.getBytes(testValue2);
		assertEquals(1, testBytes2[2]);
		int test2 = IntType.getValFromBytes(testBytes2, 0);
		assertEquals(testValue2, test2);
		
		int intValue = 1234567890;
		byte[] intBytes = IntType.getBytes(intValue);
		assertEquals(IntType.bytesPerInt, intBytes.length);
		int value = IntType.getValFromBytes(intBytes, 0);
		assertEquals(intValue, value);
	}
	
	public void testTuple() throws Exception {
		byte[] tN1b = tN1.getBytes();
		byte[] tM4b = tM4.getBytes();
		byte[] tHb = tH.getBytes();
		byte[] lnb = ln.getBytes();
		
		assertTrue(tN1.equals(s.getTupleFromBytes(tN1b, 0, tN1b.length)));
		assertTrue(tM4.equals(s.getTupleFromBytes(tM4b, 0, tM4b.length)));
		assertTrue(tH.equals(s.getTupleFromBytes(tHb, 0, tHb.length)));
		
		Tuple ln2 = s.getTupleFromBytes(lnb, 0, lnb.length);
		assertTrue(ln.equals(ln2));
		
		assertTrue(ln.isLabeledNull(0));
		assertTrue(ln2.isLabeledNull(0));
		assertTrue(ln.isLabeledNull("val"));
		assertTrue(ln2.isLabeledNull("val"));
		assertFalse(ln.isLabeledNull(2));
		assertFalse(ln2.isLabeledNull(2));
		assertEquals(ln.getLabeledNull(0), ln2.getLabeledNull("name"));
		assertEquals(ln.getLabeledNull("val"), ln.getLabeledNull(1));
	}
	
	public void testPeerID() {
		byte[] ipi1b = ipi1.getBytes();
		byte[] ipi1234567890b = ipi1234567890.getBytes();
		byte[] spiHb = spiHello.getBytes();
		byte[] spiFb = spiForeign.getBytes();
		
		AbstractPeerID ipi1d = AbstractPeerID.fromBytes(ipi1b, 0, ipi1b.length);
		AbstractPeerID ipi1234567890d = AbstractPeerID.fromBytes(ipi1234567890b, 0, ipi1b.length);
		
		AbstractPeerID spiHd = AbstractPeerID.fromBytes(spiHb);
		AbstractPeerID spiFd = AbstractPeerID.fromBytes(spiFb);
		
		assertEquals(ipi1, ipi1d);
		assertEquals(ipi1234567890, ipi1234567890d);
		assertEquals(1, ((IntPeerID) ipi1d).getID());
		assertEquals(1234567890, ((IntPeerID) ipi1234567890d).getID());
		assertEquals(spiHello, spiHd);
		assertEquals(spiForeign, spiFd);
	}
	
	public void testTxnPeerID() {
		byte[] tpi1b = tpi1.getBytes();
		byte[] tpi2b = tpi2.getBytes();
		
		TxnPeerID tpi1d = TxnPeerID.fromBytes(tpi1b, 0, tpi1b.length);
		TxnPeerID tpi2d = TxnPeerID.fromBytes(tpi2b, 0, tpi2b.length);
		
		assertEquals(tpi1, tpi1d);
		assertEquals(tpi2, tpi2d);
	}
	
	public void testDouble() {
		double test1 = 1.23456789;
		byte[] test1b = DoubleType.getBytes(test1);
		double test1d = DoubleType.getValFromBytes(test1b);
		assertEquals(test1, test1d);
		
		double test2 = Double.NaN;
		byte[] test2b = DoubleType.getBytes(test2);
		double test2d = DoubleType.getValFromBytes(test2b);
		assertTrue(Double.isNaN(test2d));

		double test3 = Double.NEGATIVE_INFINITY;
		byte[] test3b = DoubleType.getBytes(test3);
		double test3d = DoubleType.getValFromBytes(test3b);
		assertEquals(Double.NEGATIVE_INFINITY, test3d);	
	}
	
	public void testLong() {
		long test1 = 5000000000L;
		byte[] test1b = LongType.getBytes(test1);
		long test1d = LongType.getValFromBytes(test1b);
		assertEquals(test1,test1d);

		long test2 = Long.MAX_VALUE;
		byte[] test2b = LongType.getBytes(test2);
		long test2d = LongType.getValFromBytes(test2b);
		assertEquals(test2,test2d);
	}
	
	public void testDate() {
		Date test1 = new Date(2000, 7, 13);
		byte[] test1b = DateType.getBytes(test1);
		Date test1d = DateType.getValFromBytes(test1b);
		assertEquals(test1, test1d);
	}
	
	public void testUpdates() {
		byte[] ub1 = u.getBytes(Update.SerializationLevel.VALUES_ONLY);
		byte[] ub2 = u.getBytes(Update.SerializationLevel.VALUES_AND_TIDS);
		byte[] ub3 = u.getBytes(Update.SerializationLevel.ALL);
		
		byte[] insb = ins.getBytes(Update.SerializationLevel.VALUES_ONLY);
		byte[] delb = del.getBytes(Update.SerializationLevel.VALUES_ONLY);
		
		Update ud1 = Update.fromBytes(scm, ub1, 0, ub1.length);
		Update ud2 = Update.fromBytes(scm, ub2, 0, ub2.length);
		Update ud3 = Update.fromBytes(scm, ub3, 0, ub3.length);
		
		Update insd = Update.fromBytes(scm, insb, 0, insb.length);
		Update deld = Update.fromBytes(scm, delb, 0, delb.length);
		
		assertTrue(ud1.getOldVal().equals(u.getOldVal()));
		assertTrue(ud1.getNewVal().equals(u.getNewVal()));
		assertEquals(0, ud1.getTids().size());
		assertTrue(ud1.getPrevTids().isEmpty());
		assertNull(ud1.getInitialTid());
		assertEquals(tN1, ud1.getInitialVal());
		assertNull(ud1.getFromTo());
		
		assertTrue(ud2.getOldVal().equals(u.getOldVal()));
		assertTrue(ud2.getNewVal().equals(u.getNewVal()));
		Set<TxnPeerID> tids2 = ud2.getTids();
		assertEquals(2, tids2.size());
		assertTrue(tids2.contains(tpi1));
		assertTrue(tids2.contains(tpi2));
		assertEquals(tpi1, ud2.getLastTid());
		assertEquals(1, ud2.getPrevTids().size());
		assertTrue(ud2.getPrevTids().contains(tpi3));
		assertEquals(tpi2, ud2.getInitialTid());
		assertEquals(tN1, ud2.getInitialVal());
		assertNull(ud2.getFromTo());
		
		assertTrue(ud3.getOldVal().equals(u.getOldVal()));
		assertTrue(ud3.getNewVal().equals(u.getNewVal()));
		Set<TxnPeerID> tids3 = ud3.getTids();
		assertEquals(2, tids3.size());
		assertTrue(tids3.contains(tpi1));
		assertTrue(tids3.contains(tpi2));
		assertEquals(tpi1, ud3.getLastTid());
		assertEquals(1, ud3.getPrevTids().size());
		assertTrue(ud3.getPrevTids().contains(tpi3));
		assertEquals(u.getInitialTid(), ud3.getInitialTid());
		assertTrue(ud3.getInitialVal().equals(u.getInitialVal()));
		assertTrue(ud3.getFromTo().equals(u.getFromTo()));
		
		assertNull(insd.getOldVal());
		assertEquals(ins.getNewVal(), insd.getNewVal());
		assertNull(deld.getNewVal());
		assertEquals(del.getOldVal(), deld.getOldVal());
		
	}
	
	public void testTrustConditions() throws Exception {
		byte[] tcBytes = tc.getBytes(scm);
		TrustConditions tcd = new TrustConditions(tcBytes, scm);
		
		assertEquals(tc, tcd);
	}
}
