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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;


@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP, TestUtil.BROKEN_TESTNG_GROUP })
public class TestOrchestraSystem extends TestCase {

	private Peer _peer1;
	private Peer _peer2;
	SchemaIDBinding scm;
	
	@BeforeMethod
	@Before
	public void setUp () throws UnsupportedTypeException, DatabaseException
	{
		TestPeer test = new TestPeer ();
		//NPE here. See https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		test.setUp();
		_peer1 = test.getPeerTwoSchemas("peer1");
		_peer2 = test.getPeerTwoSchemas("peer2");
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
		Environment env = new Environment(f, ec);
		scm = new SchemaIDBinding(env);
		
		Map<AbstractPeerID,Integer> peerSchema = new HashMap<AbstractPeerID,Integer>();
		peerSchema.put(_peer1.getPeerId(), 0);
		List<Schema> schemas = new ArrayList<Schema>();
		schemas.addAll(_peer1.getSchemas());
		peerSchema.put(_peer2.getPeerId(), 1);
		
		scm.registerAllSchemas("test", schemas, peerSchema);
	}
	
	@org.testng.annotations.Test()
	@Test
	public void testPeerIdConflict ()
	{
		OrchestraSystem system = new OrchestraSystem (scm);
		addPeer(system, _peer1, false);
		addPeer(system, _peer1.deepCopy(), true);		
	}
	
	private void addPeer (OrchestraSystem system, Peer peer, boolean shouldFail)
	{
		
		try
		{
			system.addPeer(peer);
			assertTrue(!shouldFail);
		} catch (DuplicatePeerIdException ex)
		{
			assertTrue(shouldFail);
		}
	}
	
	
	
}
