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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the class returned by {@code
 * SqlFactories.getSqlFactory.newSqlExpression(...)}.
 * 
 * @author Sam Donnelly
 */
@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlExpression {

	/**
	 * Test method for {@code
	 * ISqlFactory.newSqlExpression(ISqlExpression.Code.NOT, someSqlExpression)}
	 * .
	 */
	public void negatedNewSqlExpression() {
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();

		ISqlExpression whereExpression = sqlFactory
				.newExpression(ISqlExpression.Code.EQ);
		whereExpression.addOperand(
				SqlFactories.getSqlFactory().newConstant("44",
						ISqlConstant.Type.NUMBER)).addOperand(
				SqlFactories.getSqlFactory().newConstant("45",
						ISqlConstant.Type.NUMBER));
		ISqlExpression notExpression = sqlFactory.newExpression(
				ISqlExpression.Code.NOT, whereExpression);
		Assert.assertEquals(SqlUtil.normalizeSqlFragment(notExpression
				.toString()), SqlUtil
				.normalizeSqlFragment("(NOT (44 = 45))"));
	}

	/**
	 * For a UDF, test {@link ISqlExpression#getOperator()}.
	 */
	public void getOperatorForUDF() {
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();

		ISqlExpression skolemStrExpression = sqlFactory
				.newExpression("SKOLEMSTR");
		Assert.assertEquals(skolemStrExpression.getOperator(), "SKOLEMSTR");

	}
}
