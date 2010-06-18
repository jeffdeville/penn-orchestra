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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;

/**
 * Test the class returned by {@code
 * SqlFactories.getSqlFactory().newSqlMove(...)}.
 * 
 * @author Sam Donnelly
 */
@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlMove {
	@Test()
	public void testSqlMove() {
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
		ISqlMove sqlMove = sqlFactory.newMove("DEST", "SOURCE", false);
		List<String> actualMoveStatements = sqlMove.toStringList();
		List<String> expectedMoveStatements = newArrayList("DROP TABLE DEST",
				"rename table SOURCE to DEST",
				"CREATE TABLE SOURCE AS (SELECT * FROM DEST WHERE (1 = 2)) DEFINITION ONLY");

		Assert.assertEquals(SqlUtil
				.normalizeSqlFragment(actualMoveStatements.get(0)
						.toString()), SqlUtil
				.normalizeSqlFragment(expectedMoveStatements.get(0)
						.toString()));

		Assert.assertEquals(SqlUtil
				.normalizeSqlFragment(actualMoveStatements.get(1)
						.toString()), SqlUtil
				.normalizeSqlFragment(expectedMoveStatements.get(1)
						.toString()));

		Assert.assertEquals(SqlUtil
				.normalizeSqlFragment(actualMoveStatements.get(2)
						.toString()), SqlUtil
				.normalizeSqlFragment(expectedMoveStatements.get(2)
						.toString()));
	}
}
