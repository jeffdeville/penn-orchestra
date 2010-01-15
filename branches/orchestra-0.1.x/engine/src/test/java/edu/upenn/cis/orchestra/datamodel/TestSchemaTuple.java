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

import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static org.testng.AssertJUnit.assertEquals;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.util.DomUtils;

@org.testng.annotations.Test(groups = {TestUtil.FAST_TESTNG_GROUP})
public class TestSchemaTuple {
	Schema s;
	Relation r;
	Tuple tN1, tM, tAl;
	DocumentBuilder db;
	
	@Before
  @BeforeMethod(groups = JUNIT4_TESTNG_GROUP)
	public void setUp() throws Exception {
		s = new Schema(getClass().getSimpleName() + "_schema");
		r = s.addRelation("R");
		r.addCol("name", new StringType(true, false,true, 10));
		r.addCol("val", new IntType(false,false));
		s.markFinished();

		tN1 = s.createTuple("R", "Nick", 1);
		//tN1.set("name", "Nick");
		//tN1.set("val", 1);
		tN1.setReadOnly();
		
		tM = s.createTuple("R", "Mark", 2);
		//tM.set("name", "Mark");
		tM.setReadOnly();
		
		tAl = s.createTuple("R", "Ann", 17);
		//tAl.set("name", "Ann");
		tAl.setLabeledNull("val", 17);
		
		db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testSimpleXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tN1.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tN1d = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tN1, tN1d);
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testNullXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tM.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tMd = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tM, tMd);
	}

	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testLabeledNullXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tAl.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tAld = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tAl, tAld);
	}
}
