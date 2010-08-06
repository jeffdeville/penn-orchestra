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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.dbms.SqlDb;

public class SqlEmptyTables {
	List<String> _tables;

	SqlEmptyTables(List<String> tables) {
		_tables = tables;
	}

	public boolean emptyTables(SqlDb db) throws SQLException {
		Calendar before = Calendar.getInstance();
		if(Config.getRunStatistics()){
			for (String t : _tables) {
				ResultSet res = db.evaluateQuery("SELECT 1 FROM " + t + " " + db.getSqlTranslator().getFirstRow());

				if (!res.next()){
					Calendar after = Calendar.getInstance();
					long time = after.getTimeInMillis() - before.getTimeInMillis();
					Debug.println("EMPTY TABLE CHECK TIME: " + time + " msec");
					db.time4EmptyChecking += time;
					res.close();
					return true;
				}
				res.close();
			}
		}
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
		Debug.println("EMPTY TABLE CHECK TIME: " + time + " msec");
		db.time4EmptyChecking += time;
		return false;
	}
}
