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

import java.util.Collections;

import junit.framework.TestCase;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.ConflictType.ConflictTypeCode;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
public class TestConflicts extends TestCase {
	Schema s;
	Relation rs;
	Tuple tN1, tN2, tN3, tM4;
	Update insN1, insN2, insN3, modN1N3, modN1N2, insM4, modN1M4;
	
	@BeforeMethod
	protected void setUp() throws Exception {
		super.setUp();
		s = new Schema(getClass().getSimpleName() + "_schema");
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(true, false, true, 10));
		rs.addCol("val", new IntType(false, false));
		rs.setPrimaryKey(Collections.singleton("name"));
		s.markFinished();

		tN1 = new Tuple(rs);
		tN1.set("name", "Nick");
		tN1.set("val", 1);
		tN2 = new Tuple(rs);
		tN2.set("name", "Nick");
		tN2.set("val", 2);
		tN3 = new Tuple(rs);
		tN3.set("name", "Nick");
		tN3.set("val", 3);
		tM4 = new Tuple(rs);
		tM4.set("name", "Mark");
		tM4.set("val", 4);

		insN1 = new Update(null, tN1);
		insN2 = new Update(null, tN2);
		insN3 = new Update(null, tN3);
		modN1N3 = new Update(tN1,tN3);
		modN1N2 = new Update(tN1,tN2);
		insM4 = new Update(null, tM4);
		modN1M4 = new Update(tN1, tM4);
	}
	
	@AfterMethod
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testNoConflict() throws DbException {
		assertNull(ConflictType.getConflictType(insN1,insM4));
	}
	
	public void testKeyConflict() throws DbException {
		assertEquals(ConflictTypeCode.KEY, ConflictType.getConflictType(insN1, insN2));
	}
	
	public void testUpdateConflict() throws DbException {
		ConflictTypeCode actual = ConflictType.getConflictType(modN1N2, modN1M4);
		assertEquals(ConflictTypeCode.UPDATE, actual);
	}
	
	public void testInitialConflict() throws DbException {
		TxnPeerID tpi = new TxnPeerID(0, new IntPeerID(0));

		// Neded to set initial value to tN1, so we can't reuse value from setUp
		insM4 = new Update(null, tN1);
		insM4.setNewVal(tM4);

		insM4.addTid(tpi);
		insN1.addTid(tpi);
		
		Update n = new Update(null, tN1);
		n.setNewVal(null);
		n.addTid(tpi);

		assertEquals(ConflictTypeCode.INITIAL, ConflictType.getConflictType(insM4, insN1));
		assertEquals(ConflictTypeCode.INITIAL, ConflictType.getConflictType(insM4, n));
		assertEquals(ConflictTypeCode.INITIAL, ConflictType.getConflictType(insN1, n));
	}

}
