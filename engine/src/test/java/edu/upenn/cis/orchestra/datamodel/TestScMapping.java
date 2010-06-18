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
import java.util.Iterator;
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
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomSkolem;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;



@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP, TestUtil.BROKEN_TESTNG_GROUP })
public class TestScMapping extends TestCase {

	
	//This test has been heavily edited to remove bean references and mau no longer work even after Bug 115 is corrected.
	private OrchestraSystem _system;
	private AtomConst _cst1;
	private AtomConst _cst2;
	private AtomVariable _varX; 
	private AtomVariable _varY;
	private AtomVariable _varZ;		
	private AtomSkolem _skolemXYZ;
	

	
	@Override
	@Before
	@BeforeMethod
	public void setUp ()
			throws DuplicateRelationIdException, DuplicateSchemaIdException, DuplicatePeerIdException, InvalidBeanException, DuplicateMappingIdException, UnsupportedTypeException,
			DatabaseException
	{
		
		TestScRelation testRel = new TestScRelation ();
		testRel.setUp();
		Schema schema1 = new Schema ("schema1", "schema1");
		//Throws NPE: See https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		schema1.addRelation(testRel._rel1.deepCopy());
		schema1.addRelation(testRel._rel2.deepCopy());
		Schema schema2 = new Schema ("schema2", "schema2");
		schema2.addRelation(testRel._rel1.deepCopy());
		schema2.addRelation(testRel._rel2.deepCopy());
		Peer peer = new Peer ("peer1", "adr1", "desc1");
		peer.addSchema(schema1);
		peer.addSchema(schema2);
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
		Environment e = new Environment(f, ec);
		
		SchemaIDBinding scm = new SchemaIDBinding(e); 
		List<Schema> schemas = new ArrayList<Schema>();
		schemas.add(schema1);
		schemas.add(schema2);
		Map<AbstractPeerID,Integer> peerMap = new HashMap<AbstractPeerID,Integer>();
		peerMap.put(peer.getPeerId(), 0);
		scm.registerAllSchemas("test", schemas, peerMap);
		
		_system = new OrchestraSystem (scm);
		_system.addPeer (peer);
		
		
		_cst1 = new AtomConst ("CST1");
		_cst2 = new AtomConst ("CST2");
		_varX = new AtomVariable ("X"); 
		_varY = new AtomVariable ("Y");
		_varZ = new AtomVariable ("Z");
		List<AtomArgument> vars = new ArrayList<AtomArgument> (3);
		vars.add (_varX);
		vars.add (_varY);
		vars.add (_cst1);
		_skolemXYZ = new AtomSkolem ("f1", vars);
		
		createMappingCorrect ();
	}
	
	private void createMappingCorrect ()
				throws DuplicateMappingIdException
	{
		List<Atom> head = new ArrayList<Atom> ();
		List<Atom> body = new ArrayList<Atom> ();
		
		Peer p = _system.getPeer("peer1");
		Schema s = p.getSchema("schema1");
		
		//head += peer1.schema1.name1(f1(X,Y,Z),X,Y,'CST1')
		List<AtomArgument> vals = new ArrayList<AtomArgument> ();
		vals.add (_skolemXYZ);
		vals.add (_varX);
		vals.add (_varY);
		vals.add (_cst1);		
		Atom atomRel = null;
		try{
			atomRel = new Atom (p, s, s.getRelation("name1"), vals);
		}catch(RelationNotFoundException e){
			atomRel = null;
		}
		head.add (atomRel);
		//head += peer1.schema1.name2(Y,X,Z,'CST2')
		vals.clear();		
		vals.add (_varY);
		vals.add (_varX);
		vals.add (_varZ);
		vals.add (_cst2);
		try{
			atomRel = new Atom (p, s, s.getRelation("name2"), vals);
		}catch(RelationNotFoundException e){
			atomRel = null;
		}
		head.add (atomRel);
		
		// body += peer1.schema2.name1(X,Y,'CST1','CST2')
		vals.clear();		
		vals.add (_varX);
		vals.add (_varY);
		vals.add (_cst1);
		vals.add (_cst2);
		s = p.getSchema("schema2");
		try{
			atomRel = new Atom (p, s, s.getRelation("name1"), vals);
		}catch(RelationNotFoundException e){
			atomRel = null;
		}
		body.add (atomRel);
		
		//body += peer2.schema2.name2(X,Z,'CST1','CST2')
		p = _system.getPeer("peer2");
		s = p.getSchema("schema2");
		vals.clear();		
		vals.add (_varX);
		vals.add (_varZ);
		vals.add (_cst1);
		vals.add (_cst2);
		try{
		atomRel = new Atom (p, s, s.getRelation("name2"), vals);
		}catch(RelationNotFoundException e){
			atomRel = null;
		}
		body.add (atomRel);
		
		Mapping mappingCorrect = new Mapping ("mapp1", "mapp1", true, 2, head, body);		
		_system.getPeer("peer1").addMapping(mappingCorrect);
	}
	
	@Test
	public void testBeanConversions ()
	{
		OrchestraSystem syst = _system.deepCopy();
		
		syst = new OrchestraSystem (syst);		
		Iterator<Mapping> itMappings = syst.getPeer("peer1").getMappings().iterator();
		Mapping mapp = itMappings.next();
		
		assertTrue(mapp.getId().equals("mapp1"));
		assertTrue(mapp.getDescription().equals("mapp1"));
		assertTrue(mapp.isMaterialized());
		assertTrue(mapp.getTrustRank()==2);
		assertTrue(mapp.getMappingHead().size()==2);
		
		Atom atom = mapp.getMappingHead().get(0); 
		assertTrue(atom.getPeer().getId().equals("peer1"));
		assertTrue(atom.getSchema().getSchemaId().equals("schema1"));
		assertTrue(atom.getRelation().getName().equals("name1"));
		assertTrue(atom.getValues().get(0) instanceof AtomSkolem);
		AtomSkolem skol = (AtomSkolem) atom.getValues().get(0);
		assertTrue(skol.getName().equals("f1"));
		assertTrue(skol.getParams().size()==3);
		assertTrue(((AtomVariable) skol.getParams().get(0)).getName().equals("X"));
		assertTrue(((AtomVariable) skol.getParams().get(1)).getName().equals("Y"));
		assertTrue(((AtomConst) skol.getParams().get(2)).getValue().equals("CST1"));
		
		assertTrue(atom.getValues().get(1) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(1)).getName().equals("X"));
		assertTrue(atom.getValues().get(2) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(2)).getName().equals("Y"));
		assertTrue(atom.getValues().get(3) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(3)).getValue().equals("CST1"));


		atom = mapp.getMappingHead().get(1);
		assertTrue(atom.getPeer().getId().equals("peer1"));
		assertTrue(atom.getSchema().getSchemaId().equals("schema1"));
		assertTrue(atom.getRelation().getName().equals("name2"));		
		assertTrue(atom.getValues().get(0) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(0)).getName().equals("Y"));
		assertTrue(atom.getValues().get(1) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(1)).getName().equals("X"));
		assertTrue(atom.getValues().get(2) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(2)).getName().equals("Z"));
		assertTrue(atom.getValues().get(3) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(3)).getValue().equals("CST2"));


		atom = mapp.getBody().get(0);
		assertTrue(atom.getPeer().getId().equals("peer1"));
		assertTrue(atom.getSchema().getSchemaId().equals("schema2"));
		assertTrue(atom.getRelation().getName().equals("name1"));
		assertTrue(atom.getValues().get(0) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(0)).getName().equals("X"));
		assertTrue(atom.getValues().get(1) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(1)).getName().equals("Y"));
		assertTrue(atom.getValues().get(2) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(2)).getValue().equals("CST1"));
		assertTrue(atom.getValues().get(3) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(3)).getValue().equals("CST2"));		
	

		atom = mapp.getBody().get(1);
		assertTrue(atom.getPeer().getId().equals("peer2"));
		assertTrue(atom.getSchema().getSchemaId().equals("schema2"));
		assertTrue(atom.getRelation().getName().equals("name2"));		
		assertTrue(atom.getValues().get(0) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(0)).getName().equals("X"));
		assertTrue(atom.getValues().get(1) instanceof AtomVariable);
		assertTrue(((AtomVariable) atom.getValues().get(1)).getName().equals("Z"));
		assertTrue(atom.getValues().get(2) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(2)).getValue().equals("CST1"));
		assertTrue(atom.getValues().get(3) instanceof AtomConst);
		assertTrue(((AtomConst) atom.getValues().get(3)).getValue().equals("CST2"));

		
	}
	
}
