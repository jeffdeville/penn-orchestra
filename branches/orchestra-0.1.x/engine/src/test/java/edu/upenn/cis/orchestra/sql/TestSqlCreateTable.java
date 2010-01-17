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

import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;

/**
 * Test the {@code ISqlCreate} returned by {@code
 * SqlFactories.getSqlFactory().newSqlColumnDef(...)}.
 * 
 * @author Sam Donnelly
 */
@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlCreateTable {

	@Test
	public void test() {
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
		List<ISqlColumnDef> columns = new LinkedList<ISqlColumnDef>();
		columns.add(sqlFactory.newColumnDef("column1", "INTEGER"));
		columns.add(sqlFactory.newColumnDef("column2", "VARCHAR(10)"));

		ISqlCreateStatement create = sqlFactory.newSqlCreateTable(
				"testTableName", columns);

		String expectedCreateTable = "create table testTableName (column1 INTEGER, column2 VARCHAR(10))";

		Assert.assertEquals(SqlUtil.normalizeSqlFragment(create.toString()),
				SqlUtil.normalizeSqlFragment(expectedCreateTable));

		ISqlCreateStatement create2 = sqlFactory.newCreateTable(
				"schemaName.testTableName", "temporary", columns,
				"not logged initially");

		String expectedCreateSchemaTable = "create temporary table schemaName.testTableName (column1 INTEGER, column2 VARCHAR(10)) not logged initially";

		Assert.assertEquals(SqlUtil.normalizeSqlFragment(create2.toString()),
				SqlUtil.normalizeSqlFragment(expectedCreateSchemaTable));

		ISqlSelect select = sqlFactory.newSelect();

		select.addFromClause(newArrayList(sqlFactory
				.newFromItem("SOURCE_TABLE")));

		ISqlCreateTable createTable = sqlFactory.newCreateTable("NEW_TABLE",
				select);

		Assert
				.assertEquals(
						SqlUtil.normalizeSqlFragment(createTable.toString()),
						SqlUtil
								.normalizeSqlFragment("create table NEW_TABLE as (select * from SOURCE_TABLE) DEFINITION ONLY;"));

	}

}
