package edu.upenn.cis.orchestra.obsolete;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;

public abstract class CreateAuxiliaryStorage {
		public abstract void createAuxiliaryDbTable (final Relation rel, boolean withNoLogging, IDb db);
}
