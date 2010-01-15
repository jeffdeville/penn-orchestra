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
package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.List;

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;

/**
 * Generates SQL strings.
 * 
 * @author John Frommeyer
 *
 */
public interface ISqlStatementGen {

	public static final String sessionSchema = "SESSION";

	public String preparedParameterProjection();

	public String nullProjection(String type);

	public String skolemNullProjection(String type);

	public String skolemColumnValue(String type);

	public String caseNull(String name);

	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db);

	public String runStats(String tableName, AtomType type, boolean detailed);

	public String getLoggingMsg();

	public List<String> turnOffLoggingAndResetStats(String tabName);

	public List<String> clearAndCopy(String oldTable, String newTable);

	public String dropTable(String tabName);

	public String subtractTables(String pos, String neg, String joinAtt);

	public String deleteTable(String tabName);

	public List<String> createTable(String tabName,
			List<? extends ISqlColumnDef> cols, boolean noLogging);

	public List<String> createTempTable(String tabName,
			List<? extends ISqlColumnDef> cols);

	public String createIndex(String indName, String tabName,
			List<? extends ISqlColumnDef> cols, boolean cluster,
			boolean noLogging);

	public List<String> addColsToTable(String tabName,
			List<? extends ISqlColumnDef> columns, boolean noLogging);


	public String copyTable(String oldName, String newName);

	public String compareTables(String table1, String table2);

	/**
	 * SQL expression to return up to 128 chars of an expression
	 * @param var
	 * @return
	 */
	public String getFirst128Chars(String var);

	public String getFirstRow();

}
