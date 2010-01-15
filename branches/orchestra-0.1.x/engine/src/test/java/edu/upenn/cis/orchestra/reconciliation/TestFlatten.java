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

import java.util.ArrayList;
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
import edu.upenn.cis.orchestra.datamodel.Update;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
public class TestFlatten extends TestCase {
	Schema s;
	Relation rsR, rsS;
	ArrayList<Update> updates;
	Tuple t1, t2, t3, t4, t5;
	IntPeerID p0, p1, p2, p3, p4, p5;
	@BeforeMethod
	protected void setUp() throws Exception{
		super.setUp();
		
		s = new Schema(getClass().getSimpleName() + "_schema");
		rsR = s.addRelation("R");
		rsR.addCol("name", new StringType(true, false, true, 10));
		rsR.addCol("val", new IntType(false, false));
		rsR.setPrimaryKey(Collections.singleton("name"));
		s.markFinished();
		
		p0 = new IntPeerID(0);
		p1 = new IntPeerID(1);
		p2 = new IntPeerID(2);
		p3 = new IntPeerID(3);
		p4 = new IntPeerID(4);
		p5 = new IntPeerID(5);
		
		updates = new ArrayList<Update>();

		t1 = new Tuple(rsR);
		t1.set("name", "Nick");
		t1.set("val", 1);
		t2 = new Tuple(rsR);
		t2.set("name", "John");
		t2.set("val", 2);
		t3 = new Tuple(rsR);
		t3.set("name", "Nick");
		t3.set("val", 3);
		t4 = new Tuple(rsR);
		t4.set("name", "Jack");
		t4.set("val", 4);
		t5 = new Tuple(rsR);
		t5.set("name", "Jack");
		t5.set("val", 5);
	}

	@AfterMethod
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testModifyDelete() throws Exception {
		Update u4 = new Update(t4, t5);
		u4.addTid(4, p1);
		Update u5 = new Update(t5, null);
		u5.addTid(5, p1);
		
		updates.add(u4);
		updates.add(u5);
		
		ArrayList<Update> flattened = Db.flatten(updates);
		Update expectedResult = new Update(t4, null);
		expectedResult.addTid(4, p1);
		expectedResult.addTid(5, p1);
		
		assertEquals("Modification and deletion should collapse into deletion",
				expectedResult, flattened.get(0));
	
	}
	
	public void testInsertModify() throws Exception {
		Update u1 = new Update(null, t1);
		u1.addTid(1, p0);
		Update u3 = new Update(t1, t3);
		u3.addTid(3, p0);

		updates.add(u1);
		updates.add(u3);
		ArrayList<Update> flattened = Db.flatten(updates);
		
		Update expectedResult = new Update(null, t1);
		expectedResult.addTid(1, p0);
		expectedResult.addTid(3, p0);
		expectedResult.setNewVal(t3);
		
		assertEquals("Insertion and modification should collapse into insertion",
			flattened.get(0), expectedResult);
	}
	
	public void testSorting() throws Exception {
		Update u1 = new Update(t2,null);
		u1.addTid(1, p3);
		Update u2 = new Update(t1, t3);
		u2.addTid(2, p4);
		Update u3 = new Update(null,t4);
		u3.addTid(3, p5);
		
		// In reverse from flattened order: insertion, update, deletion
		updates.add(u3);
		updates.add(u2);
		updates.add(u1);

		ArrayList<Update> flattened = Db.flatten(updates);
		
		Collections.reverse(updates);

		assertEquals("Flattening should result in deletions, then modifications, then insertions",
				flattened, updates);
	}
}
