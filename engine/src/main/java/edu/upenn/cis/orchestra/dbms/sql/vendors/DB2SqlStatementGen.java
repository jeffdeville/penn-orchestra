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
package edu.upenn.cis.orchestra.dbms.sql.vendors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.AbstractSqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.sql.ISqlAlter;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlCreateStatement;
import edu.upenn.cis.orchestra.sql.SqlFactories;

/**
 * 
 * @author gkarvoun
 *
 */
public class DB2SqlStatementGen extends AbstractSqlStatementGen {

	@Override
	public String preparedParameterProjection(){
		return ("CAST(? AS INTEGER)");
	}

	@Override
	public String nullProjection(String type){
		return ("cast(null as " + type + ")");
	}

	@Override
	public String skolemNullProjection(String type){
		//		return ("cast(null as " + type + ")");
		//		HACK ...
		if(type.contains("INT") || type.contains("DOUBLE") ){
			return("cast(0 as " + type + ")");
		}else{
			return("cast('empty' as " + type + ")");
		}
	}

	@Override
	public String skolemColumnValue(String type){
		//		return ("cast(null as " + type + ")");
		//		HACK ...
		if(type.contains("INT") || type.contains("DOUBLE") ){
			return("0");
		}else{
			return("'empty'");
		}
	}


	@Override
	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		List<String> statements = new ArrayList<String>();
		try{
			statements.add(fdb.getSQLImportFromFile(AtomType.DEL));
			statements.add(fdb.getSQLImportFromFile(AtomType.INS));
		}catch(Exception e){
			e.printStackTrace();
		}
		return statements;
	}

	public String rebindAllPlans(){
		return "SELECT SUBSTR('REBIND PLAN('CONCAT NAME " +
		"  CONCAT')                        ',1,45) " +
		"FROM SYSIBM.SYSPLAN;" ;
	}

	@Override
	public String runStats(String tableName, AtomType type, boolean detailed) {
		if(!Config.getRunStatistics()){
			if (!Config.getTempTables() || !tableName.contains(ISqlStatementGen.sessionSchema)){
				return "ALTER TABLE " + tableName + " VOLATILE CARDINALITY";
			}else{
				return null;
			}
		}else{
			String suffix = "";
			if(!AtomType.NONE.equals(type))
				suffix += "_" + type.toString();
			if(detailed){
				return  "call ADMIN_CMD('RUNSTATS ON TABLE " 
//				+ tableName + suffix + " WITH DISTRIBUTION ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
				+ tableName + suffix + " ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";

			}else{
				return  "call ADMIN_CMD('RUNSTATS ON TABLE " 
//				+ tableName + suffix + " ON KEY COLUMNS ALLOW WRITE ACCESS')";
//							+ tableName + suffix + " ON ALL COLUMNS ALLOW WRITE ACCESS')";
							+ tableName + suffix + " ON KEY COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
				//			+ tableName + suffix + " ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
				//			+ tableName + suffix + " WITH DISTRIBUTION ON ALL COLUMNS ALLOW WRITE ACCESS')";
				//			+ tableName + suffix + " WITH DISTRIBUTION ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
				//			return null;
			}
		}
	}


	//	@Override
	//	public String runStatistics(String tabName){
	//		return "ADMIN_CMD(RUNSTATS ON TABLE " + tabName + 
	//		" ON ALL COLUMNS AND INDEXES ALL ALLOW WRITE ACCESS SET PROFILE)";
	//	}

	@Override
	public String getLoggingMsg() {
		if (Config.getTempTables())
			return "";
		else
			return " NOT LOGGED INITIALLY";
	}

	private String getLoggingMsg4Alter() {
		return " NOT LOGGED INITIALLY";
	}

	@Override
	public List<String> turnOffLoggingAndResetStats (String tabName)
	{
		List<String> statements = new ArrayList<String>();

		if (!Config.getTempTables() || !tabName.contains(ISqlStatementGen.sessionSchema)) {
			Debug.println("ALTER TABLE " + tabName + " ACTIVATE" + getLoggingMsg4Alter());
			statements.add("ALTER TABLE " + tabName + " ACTIVATE" + getLoggingMsg4Alter());
		}
		return statements;
	}


	public String clusterIndexSuffix(boolean noLogging)
	{
		String clustSpec = "";
		clustSpec = "CLUSTER";

		//		if (noLogging){
		//			clustSpec = clustSpec + getLoggingMsg();
		//		}
		return clustSpec;
	}

	public String alterColMsg(String col, String def){
		return "ALTER COLUMN " + col + " SET DEFAULT " + def;
	}

	@Override
	public List<String> createTable(String tabName, List<? extends ISqlColumnDef> columns, boolean noLogging){
		ISqlCreateStatement cr = sqlFactory.newCreateTable(tabName, "", columns, getLoggingMsg());

		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());

		for (int i = 0; i < columns.size(); i++) {
			ISqlColumnDef s = columns.get(i);

			ISqlAlter alt = SqlFactories.getSqlFactory().newAlter(tabName);

			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + alterColMsg(s.getName(), s.getDefault()) + " ACTIVATE" + getLoggingMsg());
				else
					ret.add(alt.toString() + alterColMsg(s.getName(), s.getDefault()));		}

		return ret;
	}
	@Override
	public List<String> addColsToTable(String tabName, List<? extends ISqlColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();

		for (int i = 0; i < columns.size(); i++) {
			ISqlColumnDef s = columns.get(i);
			ISqlAlter alt = SqlFactories.getSqlFactory().newAlter(tabName);

			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()) + " ACTIVATE" + getLoggingMsg());
				else
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()));
		}

		return ret;
	}

	@Override
	public String copyTable(String oldName, String newName){
		return new String(
				"EXPORT TO \"WEBCPTAB.IXF\" OF IXF MESSAGES \"WEBCPTAB.EXM\" SELECT * FROM " + oldName + ";\n" + 
				"IMPORT FROM \"WEBCPTAB.IXF\" OF IXF MESSAGES \"WEBCPTAB.IMM\" CREATE INTO " + newName + ";\n"
				//			"EXPORT TO \"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IXF\" OF IXF MESSAGES\n" +
				//			"\"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.EXM\" SELECT * FROM " + oldName + ";\n" + 
				//			"IMPORT FROM \"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IXF\" OF IXF MESSAGES\n" +
				//			"\"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IMM\" CREATE INTO " + newName + ";\n"
		);
	}

	private String explainQuery(String queryString, int n){
		return new String("ADMIN_CMD(explain all set queryno = " + n + " for " + queryString + ")");
	}

	private String getExplainedQueryEstimatedCostAndCardinality(int n){
		return new String(
				"select S.TOTAL_COST, C.STREAM_COUNT\n" + 
				"from EXPLAIN_STATEMENT S, EXPLAIN_STREAM C\n" +
				"where S.EXPLAIN_TIME = C.EXPLAIN_TIME and C.TARGET_ID = 1\n" +
				"and S.EXPLAIN_LEVEL = 'P' and S.QUERYNO = " + n);
	}

	@Override
	public String compareTables(String table1, String table2)
	{
		return new String(
				"select COUNT(*) " +
				"from ((select * from " + table1 + " except select * from " + table2 + ") union " +
				"      (select * from " + table2 + " except select * from " + table1 + ")) AS ZZZ"
		);
	}


	/**
	 * Generate SQL to update the system catalog relations
	 * 
	 */
	private List<String> updateCatalogInfo(Relation s, int updatedCardinality, int pages,
			Map<RelationField,Integer> columnCardinalities, Map<RelationField,Integer> avgLengths) {

		List<String> ret = new ArrayList<String>();

		ret.add("UPDATE SYSSTAT.COLUMNS SET COLCARD=-1, NUMNULLS=-1 WHERE TABNAME = '" +
				s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");

		ret.add("UPDATE SYSSTAT.TABLES SET CARD=" + updatedCardinality + ", NPAGES=" + pages + 
				", FPAGES=" + pages + ", OVERFLOW=0, ACTIVE_BLOCKS=0 WHERE TABNAME = '" +
				s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");

		if (columnCardinalities != null)
			for (RelationField a : columnCardinalities.keySet()) {
				ret.add("UPDATE SYSSTAT.COLUMNS SET COLCARD=" + columnCardinalities.get(a) + ", NUMNULLS=0 WHERE TABNAME = '" +
						s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");
			}
		if (avgLengths != null)
			for (RelationField a : avgLengths.keySet()) {
				ret.add("UPDATE SYSSTAT.COLUMNS SET AVGCOLLEN=" + avgLengths.get(a) + " WHERE TABNAME = '" +
						s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");
			}
		return ret;
	}

	@Override
	public String getFirstRow() {
		return "FETCH FIRST 1 ROWS ONLY";
	}
	public String reorg(String table) {
		return "call sysproc.admin_cmd('REORG TABLE " + table + "')";
	}
	
}