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

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.AbstractSqlStatementGen;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.sql.ISqlAlter;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlCreateTable;
import edu.upenn.cis.orchestra.sql.SqlFactories;

/**
 * 
 * @author gkarvoun
 *
 */
public class HsqlSqlStatementGen extends AbstractSqlStatementGen {

	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		List<String> statements = new ArrayList<String>();

		try{
			for (String s: baseTables) {
				if (s.endsWith("_L") || (false && s.endsWith("_R"))) {
					statements.add(fdb.getSQLImportCommandHsqlDb(s, 
							db.getAttributes(s), db.getTypes(s), AtomType.DEL));

					//statements.add(updateStatCommand(s + "_DEL"));

					statements.add(fdb.getSQLImportCommandHsqlDb(s, 
							db.getAttributes(s), db.getTypes(s), AtomType.INS));

					//statements.add(updateStatCommand(s + "_INS"));
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return statements;
	}
	
	public List<String> createTable(String tabName, List<? extends ISqlColumnDef> cols, boolean noLogging){
		ISqlCreateTable cr = sqlFactory.newCreateTable(tabName, "CACHED", cols, getLoggingMsg());
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		return ret;
	}
	
	public List<String> addColsToTable(String tabName, List<? extends ISqlColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();
				
		for (int i = 0; i < columns.size(); i++) {
			ISqlColumnDef s = columns.get(i);
			
			ISqlAlter alt = SqlFactories.getSqlFactory().newAlter(tabName);
			
			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()) + " " + getLoggingMsg());
				else
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()));
		}

		return ret;
	}


}
