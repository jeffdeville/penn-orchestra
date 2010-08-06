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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.DEV_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.io.StringReader;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem.NullOrderType;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem.OrderType;

@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlSelect {

	private ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();
	private ISqlParser _parser = SqlFactories.getSqlFactory().newParser();

	public void testAddSelectFromWhereClause() {
		ISqlSelect select = _sqlFactory.newSelect();
		select.addSelectClause(newArrayList(_sqlFactory
				.newSelectItem("CAST(NULL AS VARCHAR(255))")));
		select
				.addFromClause(newArrayList(_sqlFactory
						.newFromItem("SOME_TABLE")));
		select.addWhere(_sqlFactory.newExpression(ISqlExpression.Code.EQ,
				_sqlFactory.newConstant("1", ISqlConstant.Type.NUMBER),
				_sqlFactory.newConstant("1", ISqlConstant.Type.NUMBER)));
		Assert
				.assertEquals(
						SqlUtil.normalizeSqlStatement(select.toString()),
						SqlUtil
								.normalizeSqlStatement("select cast(null as varchar(255)) from some_table where(1 = 1)"));

	}

	/**
	 * A SELECT with no WHERE clause should return {@code null} for {@code
	 * getWhere()}.
	 * 
	 * @throws ParseException
	 */
	public void noWhereTest() throws ParseException {
		String selectStr = "SELECT a FROM r;";
		_parser.initParser(new StringReader(selectStr));
		ISqlSelect noWhereSelect = (ISqlSelect) _parser.readStatement();
		assertNull(noWhereSelect.getWhere());
	}

	/**
	 * Testing detection of WHERE clauses.
	 * 
	 * @throws ParseException
	 */
	public void whereTest() throws ParseException {
		String selectStr = "SELECT a FROM r where b = c;";
		_parser.initParser(new StringReader(selectStr));
		ISqlSelect whereSelect = (ISqlSelect) _parser.readStatement();
		ISqlExp where = whereSelect.getWhere();
		assertNotNull(where);
	}

	/**
	 * Testing handling of '||' in SQL statements. This is reported in
	 * https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=122
	 * 
	 * @throws ParseException
	 */
	@Test(groups = { DEV_TESTNG_GROUP })
	public void concatTest() throws ParseException {
		String query = "SELECT a,c + 3.5,e || 'q',f FROM R WHERE a = 5 AND R.f = (e || 'de') AND b <> 17 AND d < DATE '2006-12-25';";
		_parser.initParser(new StringReader(query));
		try {
			_parser.readStatement();
		} catch (ParseException e) {
			fail("Could not parse statement with concatenation operator '||'.",
					e);
		}

	}

	/**
	 * Test basic handling of order by clause.
	 * 
	 */
	public void orderByTest() {
		final String expected = SqlUtil
				.normalizeSqlStatement("select * from some_table order by some_col, some_other_col desc nulls first");

		ISqlSelect select = _sqlFactory.newSelect();
		select.addSelectClause(newArrayList(_sqlFactory.newSelectItem("*")));
		select
				.addFromClause(newArrayList(_sqlFactory
						.newFromItem("SOME_TABLE")));
		select.addOrderBy(newArrayList(_sqlFactory.newOrderByItem(_sqlFactory
				.newConstant("SOME_COL", Type.COLUMNNAME)), _sqlFactory
				.newOrderByItem(_sqlFactory.newConstant("SOME_OTHER_COL",
						Type.COLUMNNAME), OrderType.DESC,
						NullOrderType.NULLS_FIRST)));
		final String actual = SqlUtil.normalizeSqlStatement(select.toString());
		assertEquals(actual, expected);
	}

	/**
	 * Test select with a LEAST in the the select clause.
	 */
	public void selectLeast() {
		final String expected = SqlUtil
				.normalizeSqlStatement("select least (33, 23, 10, 7) as least_value from dual");
		final ISqlSelect select = _sqlFactory.newSelect();
		final ISqlSelectItem selectItem = _sqlFactory.newSelectItem();
		selectItem.setExpression(_sqlFactory.newExpression("least", _sqlFactory
				.newConstant("33", Type.NUMBER), _sqlFactory.newConstant("23",
				Type.NUMBER), _sqlFactory.newConstant("10", Type.NUMBER),
				_sqlFactory.newConstant("7", Type.NUMBER)));
		selectItem.setAlias("least_value");
		select.addSelectClause(newArrayList(selectItem));
		select.addFromClause(newArrayList(_sqlFactory.newFromItem("dual")));
		assertEquals(SqlUtil.normalizeSqlStatement(select.toString()), expected);
	}

	/**
	 * Test select with a nested LEAST in the the select clause built with a
	 * {@link ISqlSimpleExpression}.
	 */
	public void selectNestedLeastWSimpleExpression() {
		final String expected = SqlUtil
				.normalizeSqlStatement("select least (33, least(23, 10), 7) as least_value from dual");
		final ISqlSelect select = _sqlFactory.newSelect();
		final ISqlSelectItem selectItem = _sqlFactory.newSelectItem();
		selectItem.setExpression(_sqlFactory
				.newSimpleExpression("least (33, least(23, 10), 7)"));
		selectItem.setAlias("least_value");
		select.addSelectClause(newArrayList(selectItem));
		select.addFromClause(newArrayList(_sqlFactory.newFromItem("dual")));
		assertEquals(SqlUtil.normalizeSqlStatement(select.toString()), expected);
	}
	
	/**
	 * Numerals should be recognized as expressions and not column names.
	 * 
	 */
	@Test
	public void testNumeralAsSelectItem() {
		final String expected = SqlUtil
		.normalizeSqlStatement("select 1 as C0, 345 as C1 from A");
		final ISqlSelect select = _sqlFactory.newSelect();
		ISqlSelectItem selectItem1 = _sqlFactory
		.newSelectItem("1").setAlias("C0");
		ISqlSelectItem selectItem2 = _sqlFactory.newSelectItem("345").setAlias("C1");
		ISqlFromItem fromItem = _sqlFactory.newFromItem("A");
		select.addSelectClause(newArrayList(selectItem1, selectItem2));
		select.addFromClause(Collections.singletonList(fromItem));
		assertEquals(SqlUtil.normalizeSqlStatement(select.toString()), expected);
	}
	
}
