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
package edu.upenn.cis.orchestra.datamodel;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.LocalSchemaIDBinding;
import edu.upenn.cis.orchestra.util.DomUtils;

@org.testng.annotations.Test(groups = { TestUtil.FAST_TESTNG_GROUP})
public class TestTrustConditions {

	ISchemaIDBinding scm;
	Schema ownersSchema;
	TrustConditions tc;
	AbstractPeerID ownerPeerID;
	List<Peer> peers = new ArrayList<Peer>();
	
	@Before
	@BeforeMethod(groups = JUNIT4_TESTNG_GROUP)
	public void setUp() throws Exception {
		peers.clear();

		String ownerID = "me";
		String other1ID = "512";
		String other2ID = "you";
		String other3ID = "them";
		ownerPeerID = new StringPeerID(ownerID);

		Peer owner = new Peer(ownerID, "localhost", "");
		peers.add(owner);
		Peer other1 = new Peer(other1ID, "localhost", "");
		peers.add(other1);
		Peer other2 = new Peer(other2ID, "localhost", "");
		peers.add(other2);
		Peer other3 = new Peer(other3ID, "localhost", "");
		peers.add(other3);

		Relation ownersRelation = null;
		List<Schema> schemas = newArrayList();
		Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap(); 
		for (Peer p : peers) {
			Schema s = new Schema(p.getId()
					+ "_schema");
			Relation rs = s.addRelation("R");
			rs.addCol("name", new StringType(true, true, true, 10));
			rs.addCol("occupation", new StringType(true, true, true, 10));
			rs.addCol("age", IntType.INT);
			s.markFinished();
			p.addSchema(s);
			schemas.add(s);
			if (p == owner) {
				ownersRelation = rs;
				ownersSchema = s;
			}
			peerIDToSchema.put(p.getPeerId(), s);
		}
		AssertJUnit.assertNotNull(ownersRelation);
		AssertJUnit.assertNotNull(ownersSchema);

		Predicate atMost30 = ComparePredicate.createColLit(ownersRelation,
				"age", Op.LE, 30);
		Predicate lessThan30 = ComparePredicate.createColLit(ownersRelation,
				"age", Op.LT, 30);
		Predicate sameNameAndJob = ComparePredicate.createTwoCols(
				ownersRelation, "name", Op.EQ, "occupation");
		Predicate am30AndSnaj = new AndPred(atMost30, sameNameAndJob);

		tc = new TrustConditions(ownerPeerID);

		tc.addTrustCondition(other1.getPeerId(), other1.getSchema(other1.getId()+ "_schema").getIDForName("R"), atMost30, 12);
		tc.addTrustCondition(other2.getPeerId(), other2.getSchema(other2.getId()+ "_schema").getIDForName("R"), lessThan30, 17);
		tc.addTrustCondition(other1.getPeerId(), other1.getSchema(other1.getId()+ "_schema").getIDForName("R"), sameNameAndJob, 19);
		tc.addTrustCondition(other2.getPeerId(), other2.getSchema(other2.getId()+ "_schema").getIDForName("R"), am30AndSnaj, 3);
		tc.addTrustCondition(other3.getPeerId(), other3.getSchema(other3.getId()+ "_schema").getIDForName("R"), null, 42);

		Map<AbstractPeerID, Integer> peerSchema = new HashMap<AbstractPeerID, Integer>();
		for (int i = 0; i < peers.size(); i++) {
			peerSchema.put(peers.get(i).getPeerId(), i);
		}
		scm = new LocalSchemaIDBinding(peerIDToSchema);
	}
	
	@After
	@AfterMethod
	public void tearDownSchemaBinding() throws DatabaseException {
		/*scm.clear(e);
		scm.quit();
		e.close();*/
	}
	
	@Test
	@org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testByteification() throws Exception {
		byte[] tcb = tc.getBytes(scm);
		TrustConditions tcd = new TrustConditions(tcb, scm);
		assertEquals(tc, tcd);
	}

	@Test
	@org.testng.annotations.Test(groups = {JUNIT4_TESTNG_GROUP})
	public void testXMLification() throws Exception {
		Document d = createDocument();
		Element root = d.createElement("trustConds");
		d.appendChild(root);

		tc.serialize(d, root, scm.getSchema(ownerPeerID));

		DomUtils.write(d, System.out);

		// TrustConditions tcd = TrustConditions.deserialize(root, s, owner);
		TrustConditions tcd = TrustConditions.deserialize(root, peers,
				ownerPeerID);
		assertEquals(tc, tcd);

	}
}
