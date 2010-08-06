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

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.dbms.sql.vendors.DB2SqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.vendors.HsqlSqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.vendors.OracleSqlStatementGen;

public class SqlStatementGenFactory {
	public static ISqlStatementGen createStatementGenerator() {
		if(Config.isDB2())
			return new DB2SqlStatementGen();
		else if(Config.isOracle())
			return new OracleSqlStatementGen();
		else if(Config.isHsql())
			return new HsqlSqlStatementGen();
		else
			return new GenericSqlStatementGen();
	}
}
