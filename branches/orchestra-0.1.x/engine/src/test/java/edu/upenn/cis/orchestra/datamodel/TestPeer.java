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

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;

@org.testng.annotations.Test(groups = {TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.BROKEN_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
public class TestPeer extends TestCase {

	// Use TestSchema setUp...
	private Schema _schema1;
	private Schema _schema2;
	
	@Before
	@BeforeMethod
	public void setUp () throws UnsupportedTypeException
	{
		TestSchema test = new TestSchema ();
		//NPE here. See https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		test.setUp();
		_schema1 = test.getTwoRelsSchema("schema1");
		_schema2 = test.getTwoRelsSchema("schema2");
	}
	
	
	public Peer getPeerTwoSchemas (String peerId)
	{
		Peer peer = new Peer (peerId, "127.0.0.1:4051", "test peer");
		addSchema(peer, _schema1.deepCopy(), false);
		addSchema(peer, _schema2.deepCopy(), false);
		return peer;
	}
	
	@Test
	@org.testng.annotations.Test
	public void testSchemaIdConflict ()
	{
		Peer peer = new Peer ("peer", "127.0.0.1:4051", "test peer");
		addSchema(peer, _schema1.deepCopy(), false);
		addSchema(peer, _schema1.deepCopy(), true);
		
	}
	
	private void addSchema (Peer peer, Schema schema, boolean shouldFail)
	{
		try
		{
			peer.addSchema(schema);
			assertTrue(!shouldFail);
		} catch (DuplicateSchemaIdException ex)
		{
			assertTrue(shouldFail);
		}
	}
	
}
