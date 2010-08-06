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
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;

@org.testng.annotations.Test(groups = { TestUtil.JUNIT3_TESTNG_GROUP,
		TestUtil.FAST_TESTNG_GROUP })
public class TestScRelation extends TestCase {
	// This test has been heavily edited to remove bean references and mau no
	// longer work even after Bug 115 is corrected.
	private List<RelationField> _2fields;

	protected Relation _rel1, _rel2;

	@Override
	@Before
	@BeforeMethod
	public void setUp() throws UnsupportedTypeException {
		_2fields = new ArrayList<RelationField>();
		RelationField fld = new RelationField("fld1", "fld1", false, "bool");
		_2fields.add(fld);
		fld = new RelationField("fld2", "fld2", true, "clob(1024)");
		_2fields.add(fld);

		RelationField field1 = new RelationField("field1", "descr1", false,
				"double");
		RelationField field2 = new RelationField("field2", "descr2", false,
				"double");
		RelationField field3 = new RelationField("field3", "descr3", false,
				"double");
		RelationField field4 = new RelationField("field4", "descr4", false,
				"double");
		List<RelationField> fields = new ArrayList<RelationField>();
		fields.add(field1.deepCopy());
		fields.add(field2.deepCopy());
		fields.add(field3.deepCopy());
		fields.add(field4.deepCopy());
		_rel1 = new Relation("dbCat", "dbSchem", "dbName1", "name1", "descr1",
				true, true, fields);
		fields = new ArrayList<RelationField>();
		fields.add(field1.deepCopy());
		fields.add(field2.deepCopy());
		fields.add(field3.deepCopy());
		fields.add(field4.deepCopy());
		_rel2 = new Relation("dbCat", "dbSchem", "dbName1", "name2", "descr1",
				false, true, fields);

	}

	@Test
	@org.testng.annotations.Test(groups = { TestUtil.BROKEN_TESTNG_GROUP })
	public void testBasicProperties() throws UnsupportedTypeException {
		List<RelationField> flds = new ArrayList<RelationField>(_2fields);

		Relation rel = new Relation("dbCatal", "dbSchema", "dbName", "name",
				"description", true, true, flds);
		// NPE: https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		Relation rel2 = rel.deepCopy();

		assertTrue(rel2.getDbCatalog().equals("dbCatal"));
		assertTrue(rel2.getDbSchema().equals("dbSchema"));
		assertTrue(rel2.getName().equals("name"));
		assertTrue(rel2.getDescription().equals("description"));
		assertTrue(rel2.isMaterialized());
		assertTrue(rel2.getFields().size() == 2);
		// modif list fields pour voir si chgt
		flds.remove(0);
		assertTrue(rel.getFields().size() == 2);
		assertTrue(rel2.getFields().size() == 2);
		rel.getFields().remove(0);
		assertTrue(rel2.getFields().size() == 2);

	}

	@SuppressWarnings(value = { "unused" })
	@Test
	@org.testng.annotations.Test(groups = { TestUtil.BROKEN_TESTNG_GROUP })
	public void testConstrainsDirectCreation() throws UnknownRefFieldException {
		// NPE: https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=115
		Relation rel1 = _rel1.deepCopy();
		Relation rel2 = _rel2.deepCopy();

		PrimaryKey pk1 = new PrimaryKey("pk1", rel1, new String[] { "field1",
				"field2" });
		assertTrue(pk1.getFields().size() == 2);
		rel1.setPrimaryKey(pk1);

		try {
			@edu.umd.cs.findbugs.annotations.SuppressWarnings
			PrimaryKey unknownFieldPk = new PrimaryKey("pk1", rel1,
					new String[] { "field1", "field5" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		RelationIndexUnique uk1 = new RelationIndexUnique("uk1", rel2,
				new String[] { "field1", "field2" });
		assertTrue(uk1.getFields().size() == 2);
		rel1.addUniqueIndex(uk1);
		assertTrue(rel1.getUniqueIndexes().size() == 1);
		assertTrue(rel1.getUniqueIndexes().get(0).toString().equals(
				uk1.toString()));
		rel1.removeUniqueIndex(uk1);
		assertTrue(rel1.getUniqueIndexes().size() == 0);
		rel1.addUniqueIndex(uk1);
		rel1.clearUniqueIndexes();
		assertTrue(rel1.getUniqueIndexes().size() == 0);

		try {
			@edu.umd.cs.findbugs.annotations.SuppressWarnings
			RelationIndexUnique unknownRefFieldRelIndexUniq = new RelationIndexUnique("uk1", rel2,
					new String[] { "field1", "field6" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		try {
			RelationIndexNonUnique nuidx1 = new RelationIndexNonUnique(
					"nuidx1", rel2,
					new String[] { "field1", "field5", "field6" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		RelationIndexNonUnique nuidx1 = new RelationIndexNonUnique("nuidx1",
				rel2, new String[] { "field1", "field2" });
		rel1.addNonUniqueIndex(nuidx1);
		assertTrue(rel1.getNonUniqueIndexes().size() == 1);
		assertTrue(rel1.getNonUniqueIndexes().get(0).getFields().size() == 2);

		rel1.removeNonUniqueIndex(nuidx1);
		assertTrue(rel1.getNonUniqueIndexes().size() == 0);
		rel1.addNonUniqueIndex(nuidx1);
		rel1.clearNonUniqueIndexes();
		assertTrue(rel1.getNonUniqueIndexes().size() == 0);

		ForeignKey fk1 = new ForeignKey("fk", rel2, new String[] { "field3",
				"field4" }, rel1, new String[] { "field3", "field4" });
		assertTrue(fk1.getFields().size() == 2);
		assertTrue(fk1.getRefFields().size() == 2);
		rel1.addForeignKey(fk1);
		assertTrue(rel1.getForeignKeys().size() == 1);
		assertTrue(rel1.getForeignKeys().get(0).toString().equals(
				fk1.toString()));
		rel1.removeForeignKey(fk1);
		assertTrue(rel1.getForeignKeys().size() == 0);
		rel1.addForeignKey(fk1);
		rel1.clearForeignKeys();
		assertTrue(rel1.getForeignKeys().size() == 0);

		try {
			fk1 = new ForeignKey("fk", rel2,
					new String[] { "field3", "field6" }, rel1, new String[] {
							"field3", "field4" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		try {
			@edu.umd.cs.findbugs.annotations.SuppressWarnings
			final ForeignKey fk3 = new ForeignKey("fk", rel2, new String[] {
					"field3", "field4" }, rel1, new String[] { "field3",
					"field7" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}
	}

}
