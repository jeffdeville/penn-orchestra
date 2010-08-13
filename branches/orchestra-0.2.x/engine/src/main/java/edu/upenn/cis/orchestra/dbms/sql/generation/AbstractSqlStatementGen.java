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

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlCreateIndex;
import edu.upenn.cis.orchestra.sql.ISqlCreateTempTable;
import edu.upenn.cis.orchestra.sql.ISqlDelete;
import edu.upenn.cis.orchestra.sql.ISqlDrop;
import edu.upenn.cis.orchestra.sql.ISqlDropIndex;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlMove;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;

/**
 * SQL shared across more than one supported vendor.
 * 
 * @author gkarvoun
 * @author John Frommeyer
 *
 */
public abstract class AbstractSqlStatementGen implements ISqlStatementGen {
	
	/** A factory for creating SQL. All SQL should come from this factory. */
	protected final  ISqlFactory sqlFactory = SqlFactories.getSqlFactory();

	public String caseNull(String name) {
		return ("CASE WHEN " + name + " IS NOT NULL THEN 1 ELSE -1 END");
	}

	public List<String> turnOffLoggingAndResetStats(String tabName) {
		return new ArrayList<String>();
	}

	public List<String> clearAndCopy(String oldTable, String newTable) {
	//		This may screw "volatile cardinality" - delete instead
			List<String> ret;
			ISqlMove m;
			if (false /*Config.isDB2()*/) {
				//        	Alternative: ALTER TABLE ... ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE	
				m = sqlFactory.newMove(oldTable, newTable, false);
				ret = m.toStringList();
				ret.addAll(turnOffLoggingAndResetStats(newTable));
			} else {
				m = sqlFactory.newMove(oldTable, newTable, true);
			}
			return m.toStringList();
	//		ret.add(vol);
		}

	/**
	 * Add a column to a table
	 * 
	 * @param table
	 * @param attribName
	 * @param attribType
	 * @param attribDefault
	 * @param isNullable
	 * @return
	 */
	public String addAttribute(String table, String attribName, String attribType, String attribDefault, boolean isNullable) {
		return "ALTER TABLE " + table + " ADD COLUMN " + attribName + " " + attribType + 
		((attribDefault != null) ? " default " + attribDefault : "") + 
		((!isNullable) ? " not null" : "");
	}
	
	/**
	 * Add a list of columns to a table
	 * @param table
	 * @param attribList
	 * @return
	 */
	public String addAttributeList(String table, List<AttribSpec> attribList) {
		StringBuffer ret = new StringBuffer("ALTER TABLE " + table);
		
		boolean isFirst = true;
		for (AttribSpec att : attribList) {
			if (isFirst) {
				isFirst = false;
				ret.append(" ADD COLUMN ");
			} else
				ret.append("\n ADD COLUMN ");
		
			ret.append(att.attribName + " " + att.attribType);
			if  (att.attribDefault != null)
				ret.append(" default " + att.attribDefault);
			//if (!att.isNullable)
			//	ret.append(" set not null");
		}
		
		if (isFirst)
			return "";
		
		return ret.toString();
	}
	
	public String dropAttribute(String table, String attribName) {
		return "ALTER TABLE " + table + " DROP COLUMN " + attribName;
	}
	
	public String dropAttributeList(String table, List<String> attribList) {
		StringBuffer ret = new StringBuffer("ALTER TABLE " + table);
		
		boolean isFirst = true;
		for (String att : attribList) {
			if (isFirst) {
				isFirst = false;
				ret.append(" DROP COLUMN ");
			} else
				ret.append("\n DROP COLUMN ");
			
			ret.append(att);
		}
		
		if (isFirst)
			return "";
		
		return ret.toString();
	}
	
	public String disableConstraints(String table) {
		return "SET INTEGRITY FOR " + table + " OFF";
	}
	
	public String enableConstraints(String table) {
		return "SET INTEGRITY FOR " + table + " IMMEDIATE CHECKED";
	}
	
	public String reorg(String table) {
		return "REORG TABLE " + table;
	}
	
	public String compareTables(String table1, String table2) {return "";}

	public String subtractTables(String pos, String neg, String joinAtt) {
		ISqlDelete d = sqlFactory.newDelete(pos, "R1");
		ISqlSelect q = sqlFactory.newSelect(sqlFactory.newSelectItem("1"),
				sqlFactory.newFromItem(neg + " R2"),
				sqlFactory.newExpression(ISqlExpression.Code.EQ, 
						sqlFactory.newConstant("R1." + joinAtt, ISqlConstant.Type.COLUMNNAME), 
						sqlFactory.newConstant("R2." + joinAtt, ISqlConstant.Type.COLUMNNAME)));
		
		ISqlExpression expr = sqlFactory.newExpression(ISqlExpression.Code.EXISTS, q);
		d.addWhere(expr);
		
		return d.toString();
	}

	public String copyTable(String oldName, String newName) {return "";}
	
	public String createSchema(String schemaName) {
		return "CREATE SCHEMA " + schemaName;
	}

	public String createIndex(String indName, String tabName, List<? extends ISqlColumnDef> cols,
			boolean cluster, boolean noLogging) {
				ISqlCreateIndex cr;
				if(cluster && !indName.startsWith(sessionSchema))
					cr = sqlFactory.newCreateIndex(indName, tabName, cols);
				else
					cr = sqlFactory.newCreateIndex(indName, tabName, cols);
				return cr.toString();
			}

	public String dropIndex(String indName) {
				ISqlDropIndex cr;
				cr = sqlFactory.newDropIndex(indName);
				return cr.toString();
			}

	public List<String> createTempTable(String tabName, List<? extends ISqlColumnDef> cols) {
		ISqlCreateTempTable cr = sqlFactory.newCreateTempTable(tabName, "", cols);
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		return ret;
	}

	public String deleteTable(String tabName) {
		ISqlDelete d = sqlFactory.newSqlDelete(tabName);
		return d.toString();
	}
	
	public String dropTable(String tabName) {
		ISqlDrop d = sqlFactory.newDrop(tabName);
		return d.toString();
	}	

	public String getFirst128Chars(String var) {
		if(Config.getDBMSversion() >= 9.5)
			return "SUBSTR(" + var + ",1,MIN(LENGTH(" + var + "), 128))";
		else
			return "SUBSTR(" + var + ",1,CASE WHEN LENGTH(" + var + ") > 128 THEN 128 ELSE LENGTH(" + var + ") END)";
	}

	public String getFirstRow() {
		return "LIMIT 1";
	}

	public String getLoggingMsg() {
		return "";
	}

	public String nullProjection(String type) {
		return ("null");
	}

	public String preparedParameterProjection() {
		return ("?");
	}

	public String runStats(String tableName, AtomType type, boolean detailed) {
		return null;
	}

	public String skolemColumnValue(String type) {
		return("''");
	}

	public String skolemNullProjection(String type) {
	//		return ("null");
	//		HACK ...
			if(type.contains("INT") || type.contains("DOUBLE") ){
				return("0");
			}else{
				return("''");
			}
		}

	protected String addColMsg(String col, String type, String def) {
	//		Is this DB2 specific?
			return "ADD " + col + " " + type + " DEFAULT " + def;
	}
	
}
