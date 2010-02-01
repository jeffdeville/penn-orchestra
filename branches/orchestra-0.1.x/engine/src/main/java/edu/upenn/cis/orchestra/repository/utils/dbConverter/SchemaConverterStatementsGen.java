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
package edu.upenn.cis.orchestra.repository.utils.dbConverter;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntegrityConstraint;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.RelationIndexUnique;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.SqlFactories;

public class SchemaConverterStatementsGen {

	public final static boolean INDEX_ALL_FIELDS = true;
	//	public final static String ALTER_NO_LOGGING = " ACTIVATE NOT LOGGED INITIALLY";

	// TODO: Would be better to keep indexes tablespaces but is it possible via JDBC ??
	// TODO: It also loses deferrability and this kind of qttributes for foreign keys
	// TODO: Views are not converted to deal with labeled nulls

	private DataSource _ds;
	private String _jdbcDriver;
	private Schema _sc;
	private final ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();
	private final List<String> _existingTableNames;

	////	TODO: CLEAN!!!!!!!!
	//	public enum AtomType 
	//	{
	//	NONE,
	////	OLD,
	//	NEW,
	//	INS,
	//	DEL,
	////	AFF,
	//	RCH,
	////	LOOPTEST,
	//	INV,
	//	ALLDEL
	//	};




	private Log _log = LogFactory.getLog(getClass());


	public SchemaConverterStatementsGen (DataSource ds, String jdbcDriver, Schema sc)
	{
		_ds = ds;
		_sc = sc;
		_jdbcDriver = jdbcDriver;
		_existingTableNames = getExistingTableNames();
	}

	/**
	 * Create the SQL statements necessary to fill in any missing tables in the schema
	 * 
	 * @param statements The SQL statements will be added in this list. Order must be respected for execution
	 * @return List of relations that already contain labeled null columns but without the correct characteristics
	 */
	public List<String> createNecessaryTables(boolean withNoLogging, String database,
			boolean stratified, boolean withLogging, SqlDb db)
			throws MetaDataAccessException {

		List<String> statements = new ArrayList<String>();
		if (_existingTableNames.isEmpty()) {
			for (Relation rel : _sc.getRelations()) {
				String suffix = "";
				statements.addAll(db.createSQLTableCode(rel.getDbRelName(),
						suffix, rel, false, false, false, false, withLogging));
			}

			/*
			 * // Create any key constraints for (Relation rel :
			 * _sc.getRelations()) { // First of all, recreate primary keys.
			 * Necessary for foreign keys // creation createPrimaryKey(rel,
			 * statements); // Create unique indexes createUniqueIndexes(rel,
			 * statements);
			 * 
			 * }
			 */

			// TODO: foreign keys???
		}
		return statements;

	}


	/**
	 * Create the SQL statements necessary to convert the schema's relations so 
	 * that they can store labeled nulls.
	 * @param statements The SQL statements will be added in this list. Order must be respected for execution
	 * @param containsBidirectionalMappings 
	 * @return List of relations that already contain labeled null columns but without the correct characteristics
	 */
	public Map<Relation,List<String>> createTableConversionStatements (List<String> statements,
			boolean withNoLogging, String database, boolean stratified, String NO_LOGGING,
			SqlDb db, boolean containsBidirectionalMappings) throws MetaDataAccessException {
		Map<Relation,List<String>> res;

		// First step : remove constraints that will be affected by 
		// the addition of skolems => Foreign keys, primary keys, unique indexes
		//TODO
		//removeConstraints(statements);

		// Second step : add the labeled null columns
		res = addLabeledNullColumns (statements, withNoLogging, db);

		// Third step
		//TODO
		//recreateConstraints(statements);

		// 4th step: create the delta tables 
		createDeltaTables (statements, withNoLogging, database, stratified, NO_LOGGING, db, containsBidirectionalMappings);

		return res;

	}

	/**
	 * @deprecated
	 * 
	 * @param statements
	 * @param withNoLogging
	 * @param database
	 * @param stratified
	 * @param NO_LOGGING
	 * @param db
	 */
	private void OLDcreateDeltaTables (List<String> statements, 
			boolean withNoLogging, String database, boolean stratified, String NO_LOGGING, SqlDb db)
	{


		// Modified by zives 12/20:  don't create tables for _NONE.  Instead
		// update the base table
		for (Relation rel : _sc.getRelations())
		{
			for (AtomType type : AtomType.values())
			{
				String relName = rel.getDbRelName();
				//if((relName.endsWith("_L") || relName.endsWith("_R")) && (type == AtomType.RCH)){
				if (rel.isInternalRelation() && (type == AtomType.RCH)){
					// We don't need to create _RCH tables for idbs
				}else if(!stratified || type != AtomType.ALLDEL){
					if (type != AtomType.NONE)
						relName = relName + "_" + type.toString();

					_log.debug("Delta table " + relName + ": prep statement");
					relName = relName.replaceAll("_", "\\\\_");

					if (type != AtomType.NONE) {
						if (_existingTableNames.contains(rel.getFullQualifiedDbId() + "_" + type.toString()))
							statements.add ("DROP TABLE " + rel.getFullQualifiedDbId() + "_" + type.toString());
					}

					//					String tablename = rel.getFullQualifiedDbId() + "_" + type.toString();
					String tablename = rel.getFullQualifiedDbId();

					//					String stratum = "";
					//					if(!tablename.endsWith("_L") && !tablename.endsWith("_R") && (type == AtomType.DEL)){
					//						stratum = "STRATUM INTEGER, "; 
					//					}
					String stratum = "";

					List<ISqlColumnDef> cols = newArrayList();
					if(stratified && 
							(type == AtomType.DEL || type == AtomType.INS)){

						//						SqlColumnDef s = new SqlColumnDef("STRATUM", "INTEGER", "0");

						if(Config.isOracle())
							stratum = "STRATUM INTEGER DEFAULT 0, ";
						else
							stratum = "STRATUM INTEGER, ";
					}

					if (type != AtomType.NONE)
						tablename = tablename + "_" + type.toString();

					String typ;
					//					if (database.contains("hsqldb"))
					//				typ = "CACHED ";
					//			else 
					typ = "";

					if (type != AtomType.NONE) {

						String stat = "CREATE " + typ + "TABLE " + tablename + "(" + stratum;

						for (int i = 0; i < rel.getFields().size(); i++) {
							if (i > 0)
								stat = stat + ",";
							stat = stat + rel.getField(i).getName() +
							" " + rel.getField(i).getSQLTypeName();

							cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName(), 
									rel.getField(i).getSQLTypeName(), null));
						}
						for (int i = 0; i < rel.getFields().size(); i++) {
							stat = stat + "," + rel.getField(i).getName() + RelationField.LABELED_NULL_EXT +
							" INTEGER";// + rel.getField(i).getDbType();

							if (tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS") ||
									tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL"))
								cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT, 
										"INTEGER", "1"));
							else
								cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT,
										"INTEGER", null));
						}
						stat = stat + ") ";
						/*					
								+ " AS (SELECT * FROM " + rel.getFullQualifiedDbId() + ")";				
						if (_jdbcDriver.contains("db2")) {
								statement = statement + " WITH NO DATA";

								if (!withLogging)
									statement = statement + NO_LOGGING;
						}*/
						//						if (withNoLogging)
						//							if(Config.isDB2() || Config.isOracle())
						//								statement = statement + NO_LOGGING;

						//						statements.add(stat);
						List<String> stmts = db.getSqlTranslator().createTable(tablename, cols, withNoLogging);

						statements.addAll(stmts);
					}

					//					statements.add("ALTER TABLE " + tablename + " VOLATILE CARDINALITY");


					if (tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS") ||
							tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL")){
						for (RelationField fld : rel.getFields()) {
							StringBuffer buf = new StringBuffer();
							buf.append("ALTER TABLE " + tablename);
							// Add null column if this attribute is not the key
							//							boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);


							//							if(!fldInKey){
							if(database.contains("db2")){
								buf.append(" ALTER COLUMN ");
								buf.append(fld.getName() + RelationField.LABELED_NULL_EXT);

								buf.append(" SET DEFAULT 1");
							}else{
								buf.append(" modify (");
								buf.append(fld.getName() + RelationField.LABELED_NULL_EXT);
								buf.append(" DEFAULT 1)");
							}
							//							}
							statements.add(buf.toString());
						}
					}

					if(stratified){
						if(tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL") || 
								tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS")){
							StringBuffer buf = new StringBuffer();
							buf.append("ALTER TABLE " + tablename);
							if(database.contains("db2"))
								buf.append(" ALTER COLUMN STRATUM SET DEFAULT 0");
							else
								buf.append(" modify (STRATUM DEFAULT 0)");
							statements.add(buf.toString());
						}
					}


					StringBuffer keybuf = new StringBuffer();
					boolean first = true;
					for (RelationField fld : rel.getFields()) {
						boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);
						if(fldInKey){
							if(first){
								first = false;
							}else{
								keybuf.append(",");
							}
							keybuf.append(fld.getName());
						}
					}

					String clusterSpec = "";

					//					if (!database.contains(("hsqldb")))
					if (database.contains(("db2")))
						clusterSpec = "CLUSTER";

					// TODO: remove hack
					if (!INDEX_ALL_FIELDS) {

						//						TEMPORARY HACK
						keybuf = new StringBuffer();
						keybuf.append("KID");

						String stmt = "CREATE INDEX " + tablename + "_IDX "
						+ " ON " + tablename 
						+ "(" + keybuf.toString() +") " + clusterSpec;
						statements.add (stmt);

						//						if (rel.getField("KID")!=null){
						//						statements.add ("CREATE INDEX " + tablename + "_IDX "
						//						+ " ON " + tablename 
						//						+ "(KID) CLUSTER");
						//						}

					} else {
						//						boolean first = true;
						//						StringBuffer buff = new StringBuffer ();
						//						if (rel.getField("KID")!=null)
						//						{
						//						buff.append ("KID");
						//						first = false;
						//						}
						StringBuffer buff = new StringBuffer();
						if(keybuf.length() == 0)
							first = true;
						else
							first = false;
						buff.append(keybuf.toString());
						for (RelationField fld : rel.getFields())
						{
							// Add null column if this attribute is not the key
							//							boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);

							//							if(!fldInKey){
							buff.append(first?"":",");
							buff.append(fld.getName() + RelationField.LABELED_NULL_EXT);
							//							}
							first = false;
						}			
						String stmt = "CREATE INDEX " + tablename + "_IDX "
						+ " ON " + tablename 
						+ "(" + buff.toString() + ") " + clusterSpec;
						statements.add (stmt);
						//						System.out.println("CREATE INDEX " + (rel.getDbSchema()!=null?rel.getDbSchema() + ".":"")  
						//						+ rel.getDbRelName()  + "_" + type.toString() + "_IDX "
						//						+ " ON " + rel.getFullQualifiedDbId() + "_" + type.toString() 
						//						+ "(" + buff.toString() + ") CLUSTER");
					}

					//					statements.add("ALTER TABLE " + tablename + " VOLATILE CARDINALITY");

					_log.debug("Delta table " + relName + ": statement done");

				}
			}
		}
	}

	private void createDeltaTables (List<String> statements, 
			boolean withNoLogging, String database, boolean stratified, String NO_LOGGING, SqlDb db, boolean containsBidirectionalMappings)
	{


		// Modified by zives 12/20:  don't create tables for _NONE.  Instead
		// update the base table
		for (Relation rel : _sc.getRelations())
		{
			for (AtomType type : AtomType.values())
			{
				String relName = rel.getDbRelName();
				//if((relName.endsWith("_L") || relName.endsWith("_R")) && (type == AtomType.RCH)){
				if ((rel.isInternalRelation() && (type == AtomType.RCH)) 
						|| ((!containsBidirectionalMappings) && (type == AtomType.D))
				)
					//				if ((rel.getName().endsWith(Relation.LOCAL) && (type == AtomType.RCH)) ||
					//					(rel.getName().endsWith(Relation.REJECT) && (type != AtomType.NONE)))
				{
					//					We don't need to create _RCH tables for idbs
				}else if(!stratified || type != AtomType.ALLDEL){
					if (type != AtomType.NONE)
						relName = relName + "_" + type.toString();

					_log.debug("Delta table " + relName + ": prep statement");
					relName = relName.replaceAll("_", "\\\\_");

					if (type != AtomType.NONE) {
						if (_existingTableNames.contains(rel.getFullQualifiedDbId() + "_" + type.toString()))
							statements.add ("DROP TABLE " + rel.getFullQualifiedDbId() + "_" + type.toString());
					}
					// gregkar
					String tablename;
					//					if(Config.getTempTables() 
					//							&& type != AtomType.NONE
					//							){
					//						tablename = SqlStatementGen.sessionSchema + "." + rel.getDbRelName();
					//					} else {
					tablename = rel.getFullQualifiedDbId();
					//					}

					List<ISqlColumnDef> cols = newArrayList();
					if(stratified && 
							(type == AtomType.DEL || type == AtomType.INS)){
						if(tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL") || 
								tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS")){

							cols.add(_sqlFactory.newColumnDef("STRATUM", "INTEGER", "0"));
						}else{
							cols.add(_sqlFactory.newColumnDef("STRATUM", "INTEGER", null));
						}
					}

					if (type != AtomType.NONE)
						tablename = tablename + "_" + type.toString();

					if (type != AtomType.NONE) {

						for (int i = 0; i < rel.getFields().size(); i++) {
							cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName(), 
									rel.getField(i).getSQLTypeName(), null));
						}
						if(rel.hasLabeledNulls()){
							for (int i = 0; i < rel.getFields().size(); i++) {

								if (!Config.useCompactNulls() || rel.isNullable(i)) {
									if (tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS") ||
											tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL"))
										cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT, 
												"INTEGER", "1"));
									else
										cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT, 
												"INTEGER", null));
								}
							}
						}
						// gregkar
						if (!Config.getTempTables() 
								|| !(tablename.startsWith(ISqlStatementGen.sessionSchema) // || type != AtomType.NONE
								)
						) {
							System.out.println("* Adding non-temp table " + tablename);
							statements.addAll(db.getSqlTranslator().createTable(tablename, cols, withNoLogging));
						} else {
							System.out.println("* Adding delta temp table " + tablename);
							statements.addAll(db.getSqlTranslator().createTempTable(tablename, cols));
						}

					}

					List<ISqlColumnDef> v = newArrayList();
					//					boolean first = true;
					for (RelationField fld : rel.getFields()) {
						boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);
						if(fldInKey){
							v.add(_sqlFactory.newColumnDef(fld.getName(), "", null));
						}
					}

					// TODO: remove hack
					if (!INDEX_ALL_FIELDS) {

						//						TEMPORARY HACK
						v.add(_sqlFactory.newColumnDef("KID", "", null));


					} else {
						if(rel.hasLabeledNulls()){
							int inx = 0;
							for (RelationField fld : rel.getFields())
							{
								if (!Config.useCompactNulls() || rel.isNullable(inx))
									v.add(_sqlFactory.newColumnDef(fld.getName() + RelationField.LABELED_NULL_EXT, "", null));
								
								inx++;
							}
						}

					}

					String stmt;						
					// gregkar
					stmt = db.getSqlTranslator().createIndex(tablename + "_IDX ",
							tablename, v, true, true);

					statements.add (stmt);

					_log.debug("Delta table " + relName + ": statement done");

				}
			}
		}
	}

	/**
	 * 
	 * Returns a list of the db names of any tables which already exist in the
	 * {@code Schema}.
	 * 
	 * @return a list of table names which exist in the {@code Schema}
	 */
	private List<String> getExistingTableNames() {
		_log.debug("Loading all the existing tables in memory");
		List<String> existingTables = new ArrayList<String>();
		Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
		for (Relation rel : _sc.getRelations()) {
			if (!schemas.containsKey(rel.getDbCatalog()))
				schemas.put(rel.getDbCatalog(), new HashSet<String>());
			Set<String> schemNames = schemas.get(rel.getDbCatalog());
			schemNames.add(rel.getDbSchema());
		}
		for (Map.Entry<String, Set<String>> entry : schemas.entrySet()) {
			for (String sc : entry.getValue()) {
				MetaExistTables loader = new MetaExistTables(entry.getKey(),
						sc, existingTables);
				try {
					JdbcUtils.extractDatabaseMetaData(_ds, loader);

				} catch (MetaDataAccessException ex) {
					// TODO: Actually deal with exceptions here!
					assert (false) : "not implemented yet";
				}

			}
		}
		_log.debug("Existing tables loaded. " + existingTables.size()
				+ " found");
		return existingTables;
	}



	// TODO/ What about foreign key that were not loaded in the schema??

	/**
	 * First step is to remove constraints that will be 
	 * affected by skolems introduction.
	 * They will be recreated later
	 *@param statements List to which statements must be added
	 */
	private void removeConstraints (List<String> statements)
	{
		for (Relation rel : _sc.getRelations())
		{
			for (ForeignKey fk : rel.getForeignKeys())
				removeConstraint(rel, fk, statements);
			for (RelationIndexUnique idx : rel.getUniqueIndexes())
				removeConstraint(rel, idx, statements);
			/* Let's keep the non unique indexes. skolem nulls are not discriminant enough */
			/*
			for (ScNonUniqueIndex idx : rel.getNonUniqueIndexes())
				removeIndex(rel, idx, statements);
			 */
		}
		for (Relation rel : _sc.getRelations())
			if (rel.getPrimaryKey() != null)
				removeConstraint(rel, rel.getPrimaryKey(), statements);			
	}

	/**
	 * Remove a given constraint for relation rel
	 * @param rel Relation from which the constraint has to be removed
	 * @param cst Constraint to be removed from the database
	 * @param statements List to which statements must be added
	 */
	private void removeConstraint (Relation rel, IntegrityConstraint cst,
			List<String> statements
	)
	{
		statements.add("ALTER TABLE " + rel.getFullQualifiedDbId() 
				+ " DROP CONSTRAINT " + cst.getName());
	}



	/**
	 * For each table, add a labeled null column for each 
	 * column that does not have one already
	 * @param statements List to which statements must be added 
	 * @return For each relation having "errors": list of labeled null fields already defined but incorrects
	 */
	private Map<Relation, List<String>> addLabeledNullColumns (List<String> statements,
			boolean withNoLogging, SqlDb db) throws MetaDataAccessException {
		Map<Relation, List<String>> tabErrors = new HashMap<Relation, List<String>> ();
		for (Relation rel : _sc.getRelations())
		{
			if(rel.hasLabeledNulls()){
				List<String> errors = addLabeledNullColumns (rel, statements, withNoLogging, db);
				if (errors.size()>0)
					tabErrors.put(rel, errors);
			}
		}
		return tabErrors;
	}

	/**
	 * For each column in the relation, add a new column to contain 
	 * the labeled null id.
	 * @param rel Relation for which labeled null columns must be 
	 *            added
	 * @param statements List to which new statements must be added
	 * @return List of labeled null columns already defined but incorrect
	 */
	@SuppressWarnings("unchecked")
	private List<String> addLabeledNullColumns (Relation rel,
			List<String> statements, boolean withNoLogging, SqlDb db) throws MetaDataAccessException {
		List<String> labNulls = new ArrayList<String> ();
		MetaLoadColumns loader = new MetaLoadColumns (rel.getDbCatalog(),
				rel.getDbSchema(),
				rel.getDbRelName(),
				labNulls);
		List<String> errors=null;
		//try
		//{
		errors = (List<String>) JdbcUtils.extractDatabaseMetaData(_ds, loader);
		/*
				} catch (MetaDataAccessException ex)
				{
					//TODO: Actually deal with exceptions here!
					assert (errors!=null) : "not implemented yet";			
				}*/

		//		String extra = "";
		//		if (_jdbcDriver.contains("db2")) {
		//		if (withNoLogging)
		//		extra = ALTER_NO_LOGGING;
		//		}

		List<ISqlColumnDef> cols = newArrayList();
		int inx = 0;
		for (RelationField fld : rel.getFields())
		{
			// Add null column if this attribute is not the key
			//			boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);

			//			if(!fldInKey){
			if ((!Config.useCompactNulls() || rel.isNullable(inx)) &&
					!labNulls.contains(fld.getName() + RelationField.LABELED_NULL_EXT))
				cols.add(_sqlFactory.newColumnDef(fld.getName() + RelationField.LABELED_NULL_EXT,
						"INTEGER", "1"));
			//			statements.add ("ALTER TABLE " + rel.getFullQualifiedDbId() 
			//			+ " ADD " + fld.getName() + ModelConstants.LABEL_NULL_EXT 
			//			//+ " NUMERIC (2, 0) WITH DEFAULT 1" + extra
			//			+ " INTEGER DEFAULT 1" + extra
			//			//TODO: Fix pb: if create table as select it doesn't copy default value!!! 
			//			/*+ " DEFAULT -1"
			//			+ " NOT NULL"*/);
			////			}
			inx++;
		}
		statements.addAll(db.getSqlTranslator().addColsToTable(rel.getFullQualifiedDbId(),
				cols, withNoLogging));
		return errors;
	}

	/**
	 * Recreate all the constraints, including the newly created 
	 * labeled null columns
	 * @param statements List to which SQL statements must be added
	 * 
	 * @deprecated
	 */
	private void recreateConstraints (List<String> statements)
	{
		for (Relation rel : _sc.getRelations())
		{
			// First of all, recreate primary keys. Necessary for foreign keys
			// creation
			createPrimaryKey (rel, statements);
			// Create unique indexes
			createUniqueIndexes(rel, statements);

		}
		for (Relation rel : _sc.getRelations())
		{
			// Create foreign keys
			createForeignKeys(rel, statements);			
		}
	}

	/**
	 * Recreate the relation primary key (if any), including 
	 * the new labeled null columns
	 * @param rel Relation for which to create the primary key
	 * @param statements List to which SQL statements must be added
	 */
	private void createPrimaryKey (Relation rel, List<String> statements)
	{
		PrimaryKey pk = rel.getPrimaryKey();
		if (pk != null)
		{
			String fieldsList = getCommaSepFieldsList (rel, pk.getFields());
			statements.add("ALTER TABLE " + rel.getFullQualifiedDbId()
					+ " ADD CONSTRAINT " + pk.getName()
					+ " PRIMARY KEY " + fieldsList);
		}
	}

	/**
	 * Recreate the relation unique indexes including the new 
	 * labeled null columns
	 * @param rel Relation for which to create the unique index
	 * @param statements List to which SQL statements must be added
	 */
	private void createUniqueIndexes (Relation rel, List<String> statements)
	{
		for (RelationIndexUnique idx : rel.getUniqueIndexes())
		{
			String fieldsList = getCommaSepFieldsList (rel, idx.getFields());
			statements.add("CREATE UNIQUE INDEX " + idx.getName()
					+ " ON " + rel.getFullQualifiedDbId()
					+ fieldsList + "CLUSTER");
			//			statements.add("ALTER TABLE " + idx.getName() + " VOLATILE CARDINALITY");
		}
	}

	/**
	 * Recreate the relation foreign keys including the new labeled null
	 * columns
	 * @param rel Relation for which to recreate foreign keys
	 * @param statements List to which SQL statements must be added
	 */
	private void createForeignKeys (Relation rel, List<String> statements)
	{
		for (ForeignKey fk : rel.getForeignKeys())
		{
			String fkFieldsList = getCommaSepFieldsList(rel, fk.getFields());
			String refFieldsLString = getCommaSepFieldsList(rel, fk.getRefFields());
			statements.add ("ALTER TABLE " + rel.getFullQualifiedDbId()
					+ " ADD CONSTRAINT " + fk.getName()
					+ " FOREIGN KEY " + fkFieldsList  
					+ " REFERENCES " 
					+ fk.getRefRelation().getName() 
					+ refFieldsLString);		
		}
	}

	/**
	 * Create a comma-separated list of the fields names, for each field include 
	 * the labeled null field name right after the associated field
	 * @param fields Fields to include in the list, should not contain labeled null fields
	 * @return comma-separated list of fields names
	 */
	private String getCommaSepFieldsList (Relation rel, List<RelationField> fields)
	{
		StringBuffer buff = new StringBuffer ();
		boolean first = true;
		buff.append("(");
		int inx = 0;
		for (RelationField fld : fields)
		{
			if (!first)
				buff.append (", ");
			else
				first = false;
			buff.append (fld.getName());
			if(rel.hasLabeledNulls() && (!Config.useCompactNulls() || rel.isNullable(inx))){
				buff.append (", ");
				buff.append (fld.getName() + RelationField.LABELED_NULL_EXT);
			}
			inx++;
		}
		buff.append(")");
		return buff.toString();
	}


	/**
	 * Metadata callback class used to load a table's columns and 
	 * store those information in order to know what tables/columns
	 * are already prepared for labeled nulls
	 * Based on the Spring framework / JDBC module 
	 * @author Olivier Biton
	 */
	private class MetaLoadColumns implements DatabaseMetaDataCallback  
	{
		String _catalog;
		String _schema;
		String _tableName;
		List<String> _fields;

		public MetaLoadColumns (String catalog, String schema, 
				String tableName, List<String> fields)
		{
			_catalog = catalog;
			_schema = schema;
			_tableName = tableName;
			_fields = fields;
		}

		/**
		 * @return If there are incorrect labeled null fields already
		 * defined in the database: list of those fields names
		 */
		public Object processMetaData(DatabaseMetaData dbmd) 
		throws  SQLException,
		MetaDataAccessException 
		{
			List<String> errorColumns = new ArrayList<String> ();

			ResultSet rs  = dbmd.getColumns(_catalog, _schema, _tableName, null);
			while (rs.next())
			{
				// Read column def first because of a bug in Oracle (column is stored as a long and has to be read first!)
				String colDef = rs.getString("COLUMN_DEF");

				//Ignore labeled null columns (if the database has already
				// been prepared for labeled null
				String colName = rs.getString("COLUMN_NAME");			
				if (colName.endsWith(RelationField.LABELED_NULL_EXT))
				{
					if (
							(rs.getInt("DATA_TYPE") != Types.NUMERIC
									&& rs.getInt("DATA_TYPE") != Types.DECIMAL
							)
							//TODO (cf. create table... pb with default copy_
							/*|| rs.getString("IS_NULLABLE").equals("YES")
						|| !colDef.equals("-1")
							 */							
					)
						errorColumns.add(colName);
					_fields.add (colName);										
				}
			}

			return errorColumns;
		}

	}	


	/**
	 * Metadata callback class used to load drop a table if it exists 
	 * @author Olivier Biton
	 */
	private class MetaExistTables implements DatabaseMetaDataCallback  
	{
		String _catalog;
		String _schema;
		List<String> _existingTables;

		public MetaExistTables (String catalog, String schema, 
				List<String> existingTables)
		{
			_catalog = catalog;
			_schema = schema;
			_existingTables = existingTables;
		}

		/**
		 * @return If there are incorrect labeled null fields already
		 * defined in the database: list of those fields names
		 */
		public Object processMetaData(DatabaseMetaData dbmd) 
		throws  SQLException,
		MetaDataAccessException 
		{
			ResultSet rs  = dbmd.getTables(_catalog, _schema, null, null);
			while (rs.next())
			{
				String tableName = "";
				if (_catalog!=null)
					tableName = _catalog + ".";
				if (_schema!=null)
					tableName = tableName + _schema + ".";
				tableName = tableName + rs.getString("TABLE_NAME");
				_existingTables.add (tableName);
			}
			return null;
		}
	}	


}
