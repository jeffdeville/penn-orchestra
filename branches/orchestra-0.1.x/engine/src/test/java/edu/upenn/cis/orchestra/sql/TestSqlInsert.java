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
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * @author John Frommeyer
 * 
 */
@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlInsert {

	/** For making SQL objects. */
	private ISqlFactory sqlFactory = SqlFactories.getSqlFactory();;

	/**
	 * An insert with no target columns.
	 * 
	 */
	public void testInsert() {
		ISqlInsert insert = sqlFactory.newInsert("tname");
		ISqlExp values = sqlFactory
				.newExpression(Code.COMMA)
				.addOperand(
						sqlFactory.newConstant("?",
								ISqlConstant.Type.PREPARED_STATEMENT_PARAMETER))
				.addOperand(
						sqlFactory.newConstant("?",
								ISqlConstant.Type.PREPARED_STATEMENT_PARAMETER));
		insert.addValueSpec(values);
		assertEquals(SqlUtil.normalizeSqlStatement(insert.toString()), SqlUtil
				.normalizeSqlStatement("insert into tname values (?, ?)"));
	}

	/**
	 * An insert with target columns.
	 * 
	 */
	public void testInsertWithTargetColumns() {
		ISqlInsert insert = sqlFactory.newInsert("tname");
		List<ISqlConstant> columns = newArrayList(sqlFactory.newConstant(
				"column1", ISqlConstant.Type.COLUMNNAME), sqlFactory
				.newConstant("column2", ISqlConstant.Type.COLUMNNAME));
		insert.addTargetColumns(columns);
		ISqlExp values = sqlFactory.newExpression(Code.COMMA).addOperand(
				sqlFactory.newConstant("1", ISqlConstant.Type.NUMBER))
				.addOperand(
						sqlFactory.newConstant("a", ISqlConstant.Type.STRING));
		insert.addValueSpec(values);
		assertEquals(SqlUtil.normalizeSqlStatement(insert.toString()), SqlUtil
				.normalizeSqlStatement("insert into tname (TNAME.COLUMN1, TNAME.COLUMN2) values (1, 'a')"));
	}
}
