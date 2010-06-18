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


import org.testng.annotations.BeforeMethod;
import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.AssertJUnit.assertEquals;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.util.DomUtils;

@org.testng.annotations.Test(groups = { FAST_TESTNG_GROUP })
public class TestPeerID {

	AbstractPeerID spi;
	AbstractPeerID ipi;
	
	@Before
  @BeforeMethod(groups = JUNIT4_TESTNG_GROUP)
	public void setUp() throws Exception {
		ipi = new IntPeerID(777);
		spi = new StringPeerID("¡Non più andrai, farfallone amaroso!");
	}

	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testXML() throws Exception {
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.newDocument();
		Element root = d.createElement("root");
		d.appendChild(root);
		Element p1 = d.createElement("peer");
		root.appendChild(p1);
		Element p2 = d.createElement("peer");
		root.appendChild(p2);
		spi.serialize(d, p1);
		ipi.serialize(d, p2);
		DomUtils.write(d, System.out);

		AbstractPeerID spid = AbstractPeerID.deserialize(p1);
		AbstractPeerID ipid = AbstractPeerID.deserialize(p2);
		
		assertEquals("Error decoding StringPeerID", spi, spid);
		assertEquals("Error decoding IntPeerID", ipi, ipid);
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testString() throws Exception {
		String ipis = ipi.serialize();
		AbstractPeerID ipid = AbstractPeerID.deserialize(ipis);
		assertEquals("IntPeerID deserialization failed", ipi, ipid);
		
		String spis = spi.serialize();
		AbstractPeerID spid = AbstractPeerID.deserialize(spis);
		assertEquals("StringPeerID deserialization failed", spi, spid);		
	}
}
