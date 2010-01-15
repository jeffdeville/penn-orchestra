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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;


public class TestSchema extends TestCase {

	
	private Relation _relation1;
	private Relation _relation2;
	
	@Before
	@BeforeMethod(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
	public void setUp () throws UnsupportedTypeException
	{
		RelationField field1 = new RelationField ("field1", "dfield1", false, "integer");
		RelationField field2 = new RelationField ("field2", "dfield2", false, "long");
		RelationField field3 = new RelationField ("field3", "dfield3", true, "integer");
		List<RelationField> fields = new ArrayList<RelationField> ();
		fields.add (field1); fields.add(field2); fields.add(field3);
		_relation1 = new Relation ("dbCat", "dbSchem", "dbRel", "name", "descr", true, true, fields);
		fields = new ArrayList<RelationField> ();
		fields.add (field1.deepCopy()); fields.add (field2.deepCopy()); fields.add(field3.deepCopy());
		_relation2 = new Relation ("dbCat", "dbSchem", "dbRel", "name2", "descr", true, true, fields);
	}
	
	public Schema getTwoRelsSchema (String schemaId)
	{
		Schema schema = new Schema (schemaId, schemaId + "descr");
		Relation rel1 = _relation1.deepCopy();
		Relation rel2 = _relation2.deepCopy();
		addRelation (schema, rel1, false);
		addRelation (schema, rel2, false);
		
		List<String> refFlds = new ArrayList<String> ();
		refFlds.add("field1");
		List<String> flds = new ArrayList<String> ();
		flds.add("field1");
		try
		{
			ForeignKey fk = new ForeignKey ("FK_REL1_REL2", rel1, flds, rel2, refFlds);
			rel2.addForeignKey(fk);
		} catch (UnknownRefFieldException ex)
		{
			assertTrue(false);
		}
		
		return schema;
	}

	public static void checkTwoNames (String wtdFld1, String wtdFld2, String fld1, String fld2)
	{
		if (wtdFld1.equals(fld1))
			assertTrue(wtdFld2.equals(fld2));
		else
		{
			assertTrue(wtdFld2.equals(fld1));
			assertTrue(wtdFld1.equals(fld2));
		}
	}
	
	@Test
	@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP, TestUtil.BROKEN_TESTNG_GROUP })
	public void testRelationIdConflict ()
	{
		//NPE here. See https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		Relation rel1 = _relation1.deepCopy();
		Relation rel2 = _relation1.deepCopy();
		
		Schema sc = new Schema ("schema", "schema");
		addRelation (sc, rel1, false);
		addRelation (sc, rel2, true);

		
	}

	
	private void addRelation (Schema sc, Relation rel, boolean shouldFail)
	{
		try
		{
			sc.addRelation(rel);
			assertTrue(!shouldFail);
		} catch (DuplicateRelationIdException ex)
		{
			assertTrue(shouldFail);
		}
	}
}
