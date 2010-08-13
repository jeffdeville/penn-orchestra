package edu.upenn.cis.orchestra.exchange;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;

public class ExchangeEngineFactory {
	public static BasicEngine getEngine(OrchestraSystem catalog, String type, IDb db) throws
		Exception {
		 if (type.compareToIgnoreCase("sql") == 0) {
			 return new SqlEngine((SqlDb)db, catalog);
		 } else {
			 throw new UnsupportedTypeException("Unknown database type: " + type);
		 }
	}
}
