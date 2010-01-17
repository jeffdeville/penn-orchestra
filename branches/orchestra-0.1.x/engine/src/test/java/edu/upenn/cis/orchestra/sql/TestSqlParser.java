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

import java.io.StringReader;

import org.testng.Assert;
import org.testng.annotations.Test;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;

@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlParser {
	@Test()
	public void test() throws ParseException {
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
		ISqlParser sqlParser = sqlFactory.newParser();
		sqlParser.initParser(new StringReader("select * from TEST_TABLE"));
		ISqlSelect select = (ISqlSelect) sqlParser.readStatement();
		Assert.assertEquals(SqlUtil.normalizeStatement(select.toString()),
				SqlUtil.normalizeStatement("select * from TEST_TABLE"));
		sqlParser
				.initParser(new StringReader(
						"insert into INTERPRO.ENTRY_DEL "
								+ "(select distinct CAST(? AS INTEGER) STRATUM "
								+ "from INTERPRO.ENTRY_L_DEL R0 where (not (exists "
								+ "(select 1 from INTERPRO.ENTRY_DEL R1 where "
								+ "(((R1.ENTRY_AC = R0.ENTRY_AC) and (R1.ENTRY_AC_LN = R0.ENTRY_AC_LN)) and ("
								+ "R1.NAME_LN = R0.NAME_LN))))))"));
		ISqlInsert sqlInsert = (ISqlInsert) sqlParser.readStatement();
		Assert
				.assertEquals(
						SqlUtil.normalizeStatement(sqlInsert.toString()),
						SqlUtil
								.normalizeStatement("insert into INTERPRO.ENTRY_DEL "
										+ "(select distinct CAST(? AS INTEGER) STRATUM "
										+ "from INTERPRO.ENTRY_L_DEL R0 where (not (exists "
										+ "(select 1 from INTERPRO.ENTRY_DEL R1 where "
										+ "(((R1.ENTRY_AC = R0.ENTRY_AC) and (R1.ENTRY_AC_LN = R0.ENTRY_AC_LN)) and ("
										+ "R1.NAME_LN = R0.NAME_LN))))))"));

		sqlParser
				.initParser(new StringReader(
						"DELETE FROM suppliers WHERE EXISTS( "
								+ "select customers.name from customers "
								+ "where customers.customer_id = suppliers.supplier_id  "
								+ "and customers.customer_name = 'IBM')"));
		ISqlDelete sqlDelete = (ISqlDelete) sqlParser.readStatement();
		Assert
				.assertEquals(
						SqlUtil.normalizeStatement(sqlDelete.toString()),
						SqlUtil
								.normalizeStatement("DELETE FROM suppliers WHERE EXISTS( "
										+ "select customers.name from customers "
										+ "where customers.customer_id = suppliers.supplier_id  "
										+ "and customers.customer_name = 'IBM')"));

	}
}
