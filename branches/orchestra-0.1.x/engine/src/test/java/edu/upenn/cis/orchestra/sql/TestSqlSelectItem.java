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
package edu.upenn.cis.orchestra.sql;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;

import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * @author Sam Donnelly
 */
@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlSelectItem {

	/** For making SQL objects. */
	private ISqlFactory _sqlFactory;

	/** _sqlFactory.newSqlSelectItem("*") */
	private ISqlSelectItem _allColumns;

	/** {@code "CAST(? AS INTEGER)"} */
	private static final String CAST_Q_AS_INTEGER = "CAST(? AS INTEGER)";

	/** _sqlFactory.newSqlSelectItem("CAST(? AS INTEGER)") */
	private ISqlSelectItem _castQAsInt;

	private static final String CAST_NULL_AS_VARCHAR_255 = "CAST(NULL AS VARCHAR(255))";

	/** {@code "CAST(NULL AS INT)"} */
	// private static final String CAST_NULL_AS_INT = "CAST(NULL AS INT)";
	/**
	 * Set up for the tests.
	 */
	@BeforeMethod
	void setup() {
		_sqlFactory = SqlFactories.getSqlFactory();
		_allColumns = _sqlFactory.newSelectItem("*");
		_castQAsInt = _sqlFactory.newSelectItem(CAST_Q_AS_INTEGER);
	}

	/**
	 * Test method for
	 * {@link edu.upenn.cis.orchestra.sql.ISqlSelectItem#getExpression()}.
	 */
	@Test
	public void testGetExpression() {
		assertEquals(_allColumns.getExpression(), null);

		ISqlSelectItem columnSelectItem = _sqlFactory
				.newSelectItem("COLUMN_NAME");
		ISqlExp exp = columnSelectItem.getExpression();
		assertEquals(exp.toString(), "COLUMN_NAME");
	}

	@Test
	public void testSetExpression() {
		ISqlSelectItem columnSelectItem = _sqlFactory.newSelectItem();
		ISqlExpression count = (ISqlExpression) columnSelectItem
				.setExpression(_sqlFactory.newExpression(Code.COUNT));
		// No operands means *
		assertEquals(SqlUtil.normalizeStatement(columnSelectItem.toString()),
				SqlUtil.normalizeStatement("COUNT(*)"));
	}

	/**
	 * Test method for
	 * {@link edu.upenn.cis.orchestra.sql.ISqlAliasedName#getTable()}.
	 */
	@Test
	public void testGetTable() {
		ISqlSelectItem selectItem = _sqlFactory.newSelectItem("A.B");
		assertEquals(selectItem.getTable(), "A");

		final ISqlSelectItem allColumns = _sqlFactory
				.newSelectItem("TABLE_NAME.*");
		final String table = allColumns.getTable();
		assertEquals(table, "TABLE_NAME");
		assertEquals(_castQAsInt.getTable(), null);
	}

	/**
	 * Test method for
	 * {@link edu.upenn.cis.orchestra.sql.ISqlAliasedName#getColumn()}.
	 */
	@Test
	public void testGetColumn() {
		final ISqlSelectItem selectItem = _sqlFactory.newSelectItem("A.B");
		assertEquals(selectItem.getColumn(), "B");
		assertEquals(StatementHelper.compareSQL(_castQAsInt.getColumn(),
				CAST_Q_AS_INTEGER), 0);
		assertEquals(_allColumns.getColumn(), "*");
	}

	/**
	 * Test method for
	 * {@link edu.upenn.cis.orchestra.sql.ISqlAliasedName#getAlias()} and
	 * {@link edu.upenn.cis.orchestra.sql.ISqlAliasedName#setAlias(String)}.
	 */
	@Test
	public void testGetAndSetAlias() {
		final ISqlSelectItem selectItem = _sqlFactory
				.newSelectItem("TABLE_NAME.COLUMN_NAME");
		assertEquals(selectItem.getAlias(), null);

		selectItem.setAlias("TEST_NAME_ALIAS");
		assertEquals(selectItem.getAlias(), "TEST_NAME_ALIAS");
		assertEquals(selectItem.getTable(), "TABLE_NAME");
		assertEquals(selectItem.getColumn(), "COLUMN_NAME");

		final ISqlSelectItem castNullAsVarchar = _sqlFactory
				.newSelectItem("CAST (NULL AS VARCHAR (255))");
		assertEquals(castNullAsVarchar.getAlias(), null);
		assertEquals(castNullAsVarchar.setAlias("TEST_ALIAS").getAlias(),
				"TEST_ALIAS");
		assertEquals(castNullAsVarchar.getTable(), null);

		assertEquals(StatementHelper.compareSQL(castNullAsVarchar.getColumn(),
				CAST_NULL_AS_VARCHAR_255), 0, "Wanted \""
				+ CAST_NULL_AS_VARCHAR_255 + "\" but got \""
				+ castNullAsVarchar.getColumn() + '"');
		assertEquals(_allColumns.getAlias(), null);
	}

	@Test(expectedExceptions = IllegalStateException.class)
	public void testSetAliasOnAllColumnsSelectItem() {
		_allColumns.setAlias("SHOULD_FAIL");
	}

	/**
	 * Test method for
	 * {@link edu.upenn.cis.orchestra.sql.ISqlAliasedName#toString()}.
	 */
	@Test
	public void testToString() {
		Assert.assertTrue(true, "shouldn't be used functionally");
	}

}
