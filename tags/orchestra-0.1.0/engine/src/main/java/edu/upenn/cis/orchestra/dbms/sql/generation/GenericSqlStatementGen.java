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

import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlCreateTable;

/**
 * 
 * @author gkarvoun
 *
 */
public class GenericSqlStatementGen extends AbstractSqlStatementGen {
	
		
	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen#importData(edu.upenn.cis.orchestra.exchange.flatfile.FileDb, java.util.List, edu.upenn.cis.orchestra.dbms.SqlDb)
	 */
	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		throw new RuntimeException("Unsupported database to import into");
	}
	
	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen#createTable(java.lang.String, java.util.List, boolean)
	 */
	public List<String> createTable(String tabName, List<? extends ISqlColumnDef> cols, boolean noLogging){
		ISqlCreateTable cr = sqlFactory.newCreateTable(tabName, "", cols, "");
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		return ret;
	}
	
	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen#addColsToTable(java.lang.String, java.util.List, boolean)
	 */
	public List<String> addColsToTable(String tabName, List<? extends ISqlColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();
		return ret;
	}
	
}
