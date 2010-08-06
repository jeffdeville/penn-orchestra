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

import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TransactionSource;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;

@org.testng.annotations.Test(groups = {TestUtil.FAST_TESTNG_GROUP })
public class TestTxnChain {
	Schema s;
	SchemaIDBinding scm;
	Relation rs;
	Update insM5,insJ1, modJ1N2, insM3;
	Update insN1, insM1, modM1M2, modN1N2, modM2M3;
	AbstractPeerID pid;
	TxnPeerID tpi1, tpi2, tpi3, tpi4, tpi5, tpi6, tpi7;
	Set<TxnPeerID> tail = new HashSet<TxnPeerID>();
	List<Update> txns = new ArrayList<Update>();
	Environment env;
	Tuple tJ1, tN1, tN2, tN3, tM1, tM2, tM3, tM5;
	
	@Before
  @BeforeMethod(groups = JUNIT4_TESTNG_GROUP)
	public void init() throws Exception {
		tail.clear();
		txns.clear();
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		env = new Environment(f, ec);
		scm = new SchemaIDBinding(env); 
		
		List<Schema> schemas = new ArrayList<Schema>();
		Map<AbstractPeerID,Integer> peerSchema = new HashMap<AbstractPeerID,Integer>();
		pid = new IntPeerID(17);		
		tpi1 = new TxnPeerID(1, pid);
		tpi2 = new TxnPeerID(2, pid);
		tpi3 = new TxnPeerID(3, pid);
		tpi4 = new TxnPeerID(4, pid);
		tpi5 = new TxnPeerID(5, pid);
		tpi6 = new TxnPeerID(6, pid);
		tpi7 = new TxnPeerID(7, pid);

		s = new Schema(getClass().getSimpleName() + "_schema");
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(true, false, true, 10));
		rs.addCol("val", new IntType(false,false));
		s.markFinished();
		schemas.add(s);
		peerSchema.put(pid, 0);
		scm.registerAllSchemas("test", schemas, peerSchema);


		
		
		tN1 = new Tuple(rs);
		tN1.set("name", "Nick");
		tN1.set("val", 1);
		tN1.setReadOnly();
		tN2 = new Tuple(rs);
		tN2.set("name", "Nick");
		tN2.set("val", 2);
		tN2.setReadOnly();
		tM1 = s.createTuple("R", "Mark", 1);
		//tM1.set("name", "Mark");
		//tM1.set("val", 1);
		tM1.setReadOnly();
		tM2 = s.createTuple("R", "Mark", 2);
		//tM2.set("name", "Mark");
		//tM2.set("val", 2);
		tM2.setReadOnly();
		tM3 = new Tuple(rs);
		tM3.set("name", "Mark");
		tM3.set("val", 3);
		tM3.setReadOnly();
		tM5 = new Tuple(rs);
		tM5.set("name", "Mark");
		tM5.set("val", 5);
		tM5.setReadOnly();
		tJ1 = new Tuple(rs);
		tJ1.set("name", "James");
		tJ1.set("val", 1);
		tJ1.setReadOnly();
		
		
		insN1 = new Update(null, tN1);
		insN1.addTid(tpi5);
		insM1 = new Update(null, tM1);
		insM1.addTid(tpi6);
		
		modN1N2 = new Update(tN1, tN2);
		modN1N2.addTid(tpi1);
		modN1N2.addPrevTid(insN1.getLastTid());
		insM5 = new Update(null, tM5);
		insM5.addTid(tpi2);

		insJ1 = new Update(null, tJ1);
		insJ1.addTid(tpi3);
		modJ1N2 = new Update(tJ1, tN2);
		modJ1N2.addTid(tpi4);
		modJ1N2.addPrevTid(insJ1.getLastTid());
		insM3 = new Update(null, tM3);
		insM3.addTid(insJ1.getLastTid());

		modM1M2 = new Update(tM1, tM2);
		modM1M2.addTid(modN1N2.getLastTid());
		modM1M2.addPrevTid(insM1.getLastTid());
		
		modM2M3 = new Update(tM2, tM3);
		modM2M3.addTid(tpi7);
		modM2M3.addPrevTid(modM1M2.getLastTid());
		
		
		tail.add(tpi2);
		txns.add(modN1N2);
		txns.add(insM5);
		

	}

	@After
	@AfterMethod
	public void tearDownSchemaBinding() throws DatabaseException {
		scm.clear(env);
		scm.quit();
		env.close();
	}
	
	
	@Test
	@org.testng.annotations.Test(groups = {JUNIT4_TESTNG_GROUP})
	public void testTxnChainSerialization() throws Exception {
		TxnChain tc = new TxnChain(txns, tpi1, tail, false);
		byte[] bytes = tc.getBytes(true);
		TxnChain tc2 = TxnChain.fromBytes(scm, bytes, 0, bytes.length);
		assertEquals(tc.getContents(), tc2.getContents());
		assertEquals(tc.getHead(), tc2.getHead());
		assertEquals(tc.getTail(), tc2.getTail());
	}
	
	@Test
	@org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testTxnChainWithGraph() throws Exception {
		TxnChain tc = new TxnChain(txns, tpi1, tail, true);
		tc.addAntecedent(tpi1, tpi2);
		byte[] bytes = tc.getBytes(false);
		TxnChain tc2 = TxnChain.fromBytes(scm, bytes, 0, bytes.length);
		assertNull(tc2.getContents());
		assertEquals(tc.getHead(), tc2.getHead());
		assertEquals(tc.getTail(), tc2.getTail());
		Set<TxnPeerID> antecedents = tc.getAntecedents(tpi2);
		Set<TxnPeerID> dependents = tc.getDependents(tpi1);
		Set<TxnPeerID> antecedents2 = tc2.getAntecedents(tpi2);
		Set<TxnPeerID> dependents2 = tc2.getDependents(tpi1);
		assertEquals(antecedents, antecedents2);
		assertEquals(dependents, dependents2);
		assertTrue(antecedents2.contains(tpi1));
		assertEquals(1,antecedents2.size());
		assertTrue(dependents2.contains(tpi2));
		assertEquals(1,dependents2.size());
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testSimpleCreateTxnChain() throws Exception {
		TransactionDecisions td = new TransactionDecisions() {
			public boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
				return false;
			}

			public boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
				return false;
			}
		};
		final HashMap<TxnPeerID, List<Update>> txnStore = new HashMap<TxnPeerID, List<Update>>();
		txnStore.put(insJ1.getLastTid(), new ArrayList<Update>());
		txnStore.put(modJ1N2.getLastTid(), new ArrayList<Update>());
		txnStore.get(insJ1.getLastTid()).add(insJ1);
		txnStore.get(modJ1N2.getLastTid()).add(modJ1N2);
		txnStore.get(insM3.getLastTid()).add(insM3);


		TransactionSource ts = new TransactionSource() {
			public List<Update> getTxn(TxnPeerID tpi) throws USException {
				return txnStore.get(tpi);
			}
			
		};

		TxnChain tc = new TxnChain(modJ1N2.getLastTid(), ts, td);
		assertEquals(modJ1N2.getLastTid(), tc.getHead());
		assertEquals(0, tc.getTail().size());
		assertEquals(3, tc.getContents().size());
		assertEquals(insJ1, tc.getContents().get(0));
		assertEquals(insM3, tc.getContents().get(1));
		assertEquals(modJ1N2, tc.getContents().get(2));
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAddToExistingChain() throws Exception {
		final HashMap<TxnPeerID, List<Update>> txnStore = new HashMap<TxnPeerID, List<Update>>();
		txnStore.put(insN1.getLastTid(), new ArrayList<Update>());
		txnStore.put(modM1M2.getLastTid(), new ArrayList<Update>());
		txnStore.put(modM2M3.getLastTid(), new ArrayList<Update>());
		txnStore.get(insN1.getLastTid()).add(insN1);
		txnStore.get(modM1M2.getLastTid()).add(modM1M2);
		txnStore.get(modM1M2.getLastTid()).add(modN1N2);
		txnStore.get(modM2M3.getLastTid()).add(modM2M3);

		TransactionSource ts = new TransactionSource() {
			public List<Update> getTxn(TxnPeerID tpi) {
				return txnStore.get(tpi);
			}
		};

		TransactionDecisions td1 = new TransactionDecisions() {
			public boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
				if (tpi.equals(modM1M2.getLastTid())) {
					return true;
				} else {
					return false;
				}
			}

			public boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
				return false;
			}
		};
		
		TxnChain tc = new TxnChain(modM2M3.getLastTid(), ts, td1);
		assertEquals("Incorrect tail to generated TxnChain", Collections.singleton(modM1M2.getLastTid()), tc.getTail());
		assertEquals("Incorrect components to generated TxnChain", Collections.singleton(modM2M3.getLastTid()), tc.getComponents());
		assertEquals("Incorrect content of generated TxnChain", Collections.singletonList(modM2M3), tc.getContents());
		tc.replaceTailWithAvailableTxns(ts);
		assertEquals("Incorrect tail to generated TxnChain", Collections.singleton(insM1.getLastTid()), tc.getTail());
	}
}
