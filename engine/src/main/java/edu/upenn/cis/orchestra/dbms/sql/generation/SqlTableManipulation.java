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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import schemacrawler.schema.Column;
import schemacrawler.schema.Database;
import schemacrawler.schema.Index;
import schemacrawler.schema.IndexColumn;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.InclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.utility.SchemaCrawlerUtility;
import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleRelationException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationUpdateException;
import edu.upenn.cis.orchestra.datamodel.exceptions.SchemaNotFoundException;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;

/**
 * Basic utility classes for tables, etc.
 * 
 * @author zives
 *
 */
public class SqlTableManipulation {
	private static SchemaCrawlerOptions options = null;
	
	private static HashMap<String,Schema> schemaCache = new HashMap<String,Schema>();
	
	private static final SchemaInfoLevel basicInfoLevel;
	
	/** Logger. */
	private static final Logger _logger = LoggerFactory
			.getLogger(SqlTableManipulation.class);

	static {
		basicInfoLevel = SchemaInfoLevel.minimum();
		basicInfoLevel.setRetrieveColumnDataTypes(false);//true);
		basicInfoLevel.setRetrieveProcedureColumns(false);//true);
		basicInfoLevel.setRetrieveIndices(true);
		basicInfoLevel.setRetrieveTableColumns(true);
	}
	
	/**
	 * Checks a set of tables to ensure they are all empty
	 * 
	 * @param db
	 * @param tables
	 * @return
	 * @throws SQLException
	 */
	public static boolean areTablesEmpty(SqlDb db, List<String> tables) throws SQLException {
		Calendar before = Calendar.getInstance();
		if(Config.getRunStatistics()){
			for (String t : tables) {
				ResultSet res = db.evaluateQuery("SELECT 1 FROM " + t + " " + db.getSqlTranslator().getFirstRow());

				if (!res.next()){
					Calendar after = Calendar.getInstance();
					long time = after.getTimeInMillis() - before.getTimeInMillis();
					_logger.info("EMPTY TABLE CHECK TIME: {} msec", time);
					db.time4EmptyChecking += time;
					res.close();
					return true;
				}
				res.close();
			}
		}
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
		_logger.info("EMPTY TABLE CHECK TIME: {} msec", time);
		db.time4EmptyChecking += time;
		return false;
	}


	/**
	 * Check that the logical relation actually has a physical relation in SQL, which occurs
	 * in the specified schema/etc. and also has compatible columns and types.
	 *  
	 * @param dbms
	 * @param rc
	 * @return
	 * @throws RelationUpdateException
	 */
	public static boolean ensureRelationExists(IDb dbms, Relation rc) throws SchemaNotFoundException, IncompatibleRelationException {
		if (dbms instanceof SqlDb)
			return ensureRelationExists(((SqlDb)dbms).getConnection(), rc);
		else
			throw new SchemaNotFoundException("Unsupported DBMS type!");
	}

	/**
	 * Check that the logical relation actually has a physical relation in SQL, which occurs
	 * in the specified schema/etc. and also has compatible columns and types.
	 *  
	 * @param dbms
	 * @param rc
	 * @param suffix String suffix (e.g., "TYP") without leading "_"
	 * @return
	 * @throws RelationUpdateException
	 */
	public static boolean ensureRelationExists(IDb dbms, Relation rc, String suffix) throws SchemaNotFoundException, IncompatibleRelationException {
		if (dbms instanceof SqlDb)
			return ensureRelationExists(((SqlDb)dbms).getConnection(), rc, suffix);
		else
			throw new SchemaNotFoundException("Unsupported DBMS type!");
	}
	
	/**
	 * Check that the logical relation actually has a physical relation in SQL, which occurs
	 * in the specified schema/etc. and also has compatible columns and types.
	 *  
	 * @param dbms
	 * @param rc
	 * @return
	 * @throws RelationUpdateException
	 */
	public static boolean ensureRelationExists(Connection dbms, Relation rc) throws SchemaNotFoundException, IncompatibleRelationException {
		return ensureRelationExists(dbms, rc, "");
	}

	/**
	 * Check whether the logical Relation definition and the actual SQL table are properly synced
	 * 
	 * @param rel Logical relation
	 * @param t Actual SQL table, from SchemaCrawler
	 * @return true if the two are NOT in sync
	 */
	public static boolean isMismatched(Relation rel, Table t) {
		int lns = 0;
		for (int col = 0; col < rel.getNumCols(); col++)
			if (rel.isNullable(col))
				lns++;

		boolean isMismatched = false;
		//if (t.getColumns().length != rel.getNumCols() + lns) {
		if (t.getColumns().length != rel.getFields().size()) {
			_logger.debug("Actual schema: {}", t.getColumnsListAsString());
//			Debug.println("ACTUAL: " + t.getColumnsListAsString());
			//Debug.println("LOGICAL: " + rel.getColumnsAsString());
			//Debug.print("PHYSICAL: ");
			List<RelationField> rfl = rel.getFields();
			boolean first = true;
			StringBuffer str = new StringBuffer();
			for (int i = 0; i < rfl.size(); i++) {
				if (!first)
					str.append(",");
				else
					first = false;
				str.append(rfl.get(i).getName() + ":" + rfl.get(i).getSQLType());
				if (rfl.get(i).getDefaultValueAsString() != null)
					str.append(" (" + rfl.get(i).getDefaultValueAsString() + ") ");
			}
			_logger.debug("Expected physical schema: {}", str);
			isMismatched = true;
		}
		
		for (int col = 0; col < rel.getNumCols(); col++) {
			String nam = rel.getColName(col);
			if (t.getColumn(nam.toUpperCase()) == null) {
				_logger.debug("* Missing attribute: {}", nam);
//				Debug.println("* Cannot find attribute " + nam + " which should have default value " + rel.getField(nam).getDefaultValueAsString());
				isMismatched = true;
			} else {
				if ((rel.isNullable(col) && t.getColumn(nam.toUpperCase() + RelationField.LABELED_NULL_EXT) == null)
						|| (!rel.isNullable(col) && t.getColumn(nam.toUpperCase() + RelationField.LABELED_NULL_EXT) != null)) {
					_logger.debug("* Mismatch with labeled null: {}", nam);
//					Debug.println("* Mismatch with labeled null: " + nam);
					isMismatched = true;
				}// else
				//	Debug.println("Found labeled null for " + nam);
				//Debug.println("Type for " + nam + ": " + rc.getColType(col) + " vs " + t.getColumn(nam.toUpperCase()).getType());
			}
		}
		
		return isMismatched;
	}
	
	/**
	 * Check that the logical relation actually has a physical relation in SQL, which occurs
	 * in the specified schema/etc. and also has compatible columns [and TODO: perhaps also types?].
	 *  
	 * @param dbms
	 * @param rc
	 * @param suffix Optional relation suffix ("_" gets added if it's non-empty)
	 * @return
	 * @throws RelationUpdateException
	 */
	public static boolean ensureRelationExists(Connection dbms, Relation rc, String suffix) throws SchemaNotFoundException, IncompatibleRelationException {
		try {
			Schema schema = schemaCache.get(rc.getDbSchema());
			if (schema == null) {
				// Get the schema definition
				if (options == null) {
					// Create the options
					options = new SchemaCrawlerOptions();
					// Set what details are required in the schema - this affects the
					// time taken to crawl the schema
					options.setSchemaInfoLevel(basicInfoLevel);
					options.setShowStoredProcedures(false);
					options.setTableTypes("TABLE,VIEW");
				}
				options.setSchemaInclusionRule(new InclusionRule(Pattern.compile(rc.getDbSchema()), Pattern.compile("")));
				
				final Database database = SchemaCrawlerUtility.getDatabase(dbms, options);

				schema = database.getSchema(rc.getDbSchema());
				if (schema != null) {
					schemaCache.put(rc.getDbSchema(), schema);
				}
				
			}
	
			if (schema != null) {
				Table t;
				String findThis = /* rc.getDbSchema() + "." +*/ rc.getDbRelName();
				
				if (!suffix.isEmpty())
					findThis = findThis + "_" + suffix;
				
				findThis = findThis.toUpperCase();

				t = schema.getTable(findThis);
				
				if (t != null) {

					/*
					int count = 0;
					for (Column c : t.getColumns()) {
						String nam = c.getName();
						if (count++ < rc.getNumCols())
							if (c.getType() != rc.getColType(nam))
								throw new IncompatibleRelationException("Unexpected type for " + nam);
					}*/
					if (isMismatched(rc, t))
						throw new IncompatibleRelationException("Table mismatch: " + rc.getQualifiedName(""), t);
					return true;
				}
			} else
				throw new SchemaNotFoundException("Unable to find DBMS schema " + rc.getQualifiedName(""));
		} catch (SchemaCrawlerException sc) {
			throw new SchemaNotFoundException("Error crawling schema: " + sc.getMessage());
		}
		return false;
	}

	/**
	 * Creates the desired table, optionally with a suffix
	 * @param rel
	 * @param type
	 * @param withNoLogging
	 * @param db
	 */
	public static List<String> createAuxiliaryDbTable (final Relation rel, Atom.AtomType type, boolean withNoLogging, 
			SqlDb db) {
		final List<String> toApply = new ArrayList<String>();
		//SqlDb d = (SqlDb)db;
		List<RelationField> indexes =  rel.getPrimaryKey().getFields();
	
		boolean relExists = false;
		String suffix = Atom.typeToString(type);
		
		_logger.debug("Creating or validating table {}", rel.getQualifiedName(suffix) + ": " + rel.getColumnsAsString());

		// See if we can find the table already
		try {
			relExists = SqlTableManipulation.ensureRelationExists(db, rel, suffix);
		} catch (SchemaNotFoundException e) {
			
			String str = createRDBMSSchema(db, rel);
			
			// Create the schema, since it's missing, unless we already have
			// a statement to create it
			if (!toApply.contains(str))
				toApply.add(str);
			
		} catch (IncompatibleRelationException e) {
			
			// Fix the table's schema, if possible
			toApply.addAll(repairDbTable(db, rel, suffix, e.getTable(), withNoLogging));
			relExists = true;
		}
		
		// Create table and index
		if (!relExists) {
			_logger.debug("Creating table from scratch...");
			toApply.addAll(db.createSQLTableCode( 
					suffix, rel, false, !withNoLogging));
	
			toApply.addAll(db.createSQLIndexCode(suffix, rel, indexes, false, !withNoLogging));
		} 
	
		if (!toApply.isEmpty()) {
			_logger.debug("SQL statements:");
			for (final String s: toApply) {
				_logger.debug(s);
			}
//			Debug.println("---");
		}
		
		return toApply;
	}

	/**
	 * Creates the complete set of auxiliary tables for a peer relation
	 * 
	 * @param rel
	 * @param withNoLogging
	 * @param db
	 * @return
	 */
	public static List<String> createAuxiliaryDbTableSet (final Relation rel, boolean withNoLogging, SqlDb db) {
		final List<String> toApply = new ArrayList<String>();
		
		for (final Atom.AtomType type : Atom.AtomType.values()){
			//String suffix = Atom.typeToString(type);
			
			//boolean stratified =  (Config.getStratified() &&
			//		   (type == AtomType.DEL || type == AtomType.INS));
	
			if ((rel.isInternalRelation() && (type == AtomType.RCH)) 
					|| ((type == AtomType.D) || (!Config.getStratified() && type == AtomType.ALLDEL))
			)
			{
				//					We don't need to create _RCH tables for idbs
				//					or D tables except for bidirectional mapping provenance tables
			} else { //if (!stratified || type != AtomType.ALLDEL){
				
				boolean addStratum = (type == AtomType.DEL) || (type == AtomType.INS);//rel.getName().endsWith(Relation.INSERT) || rel.getName().endsWith(Relation.DELETE);
				if (addStratum)
					for (RelationField f : rel.getFields()) {
						if (f.getName().equals("STRATUM")) {
							addStratum = false;
							break;
						}
					}
				if (addStratum) {
					Relation rel2 = rel.deepCopyFull();
					rel2.deepCopyFks(rel, null);
					int inx = rel.getFields().size();
					try {
						rel2.addField(new RelationField("STRATUM", "", IntType.INT));
					} catch (BadColumnName e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					rel2.getField(inx).setDefaultValueAsString("0");
					rel2.markFinished();
					toApply.addAll(createAuxiliaryDbTable(rel2, type, withNoLogging, db));//stratified, db));
				} else
					toApply.addAll(createAuxiliaryDbTable(rel, type, withNoLogging, db));//stratified, db));
			}
		}
		
		return toApply;
	}

	/**
	 * Creates the set of provenance relations for an outer union situation
	 * 
	 * @param rel
	 * @param withLogging
	 * @param db
	 * @return
	 */
	public static List<String> createOuterUnionProvenanceDbTableSet(final ProvenanceRelation rel, 
				boolean withLogging,
				SqlDb db) {

			final List<String> toApply = new ArrayList<String>();
			//final List<ScField> fields = rel.getFields();
	//		Set<Integer> indexes = new HashSet<Integer>();
			List<RelationField> indexes =  rel.getPrimaryKey().getFields();
			SqlDb d = (SqlDb)db;
						
			rel.markFinished();
			
			for (final AtomType type : AtomType.values()){
				if((type != AtomType.RCH) && (!Config.getStratified() || type != AtomType.ALLDEL)){
					toApply.addAll(createAuxiliaryDbTable(rel, type, !withLogging, //Config.getStratified() &&
							   //(type == AtomType.DEL || type == AtomType.INS), 
							db));
				}
			}
					
			for (final String s: toApply) {
				_logger.debug(s);
			}
	
			/*
			if (Config.getApply()) {
				for (final String s: toApply) {
					((SqlDb)db).evaluate(s);
				}
			}*/
			
			return toApply;
		}

	/**
	 * Creates the set of provenance tables
	 * @param rel
	 * @param withLogging
	 * @param db
	 * @param containsBidirectionalMappings
	 * @return
	 */
	public static List<String> createProvenanceDbTableSet (final Relation rel, boolean withLogging, SqlDb db, 
			boolean containsBidirectionalMappings)
	{
		final List<String> toApply = new ArrayList<String>();
		for (final Atom.AtomType type : Atom.AtomType.values()){
			if(type != AtomType.RCH && 
					(!Config.getStratified() || type != AtomType.ALLDEL)
					&& (containsBidirectionalMappings || type != AtomType.D)
					){
				
				boolean addStratum = (type == AtomType.DEL) || (type == AtomType.INS);//rel.getName().endsWith(Relation.INSERT) || rel.getName().endsWith(Relation.DELETE);
				if (addStratum)
					for (RelationField f : rel.getFields()) {
						if (f.getName().equals("STRATUM")) {
							addStratum = false;
							break;
						}
					}
				if (addStratum) {
					Relation rel2 = rel.deepCopyFull();
					rel2.deepCopyFks(rel, null);
					int inx = rel.getFields().size();
					try {
						rel2.addField(new RelationField("STRATUM", "", IntType.INT));
					} catch (BadColumnName e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					rel2.getField(inx).setDefaultValueAsString("0");
					rel2.markFinished();
					toApply.addAll(createAuxiliaryDbTable(rel2, type, !withLogging, //Config.getStratified() &&
							   //(type == AtomType.DEL || type == AtomType.INS), 
							db));
				} else
					toApply.addAll(createAuxiliaryDbTable(rel, type, !withLogging, //Config.getStratified() &&
							   //(type == AtomType.DEL || type == AtomType.INS), 
							db));
			}
		}
	
		for (final String s: toApply) {
			_logger.debug(s);
		}
	
		return toApply;
	}

	/**
	 * Returns a SQL statement to create a missing schema
	 * 
	 * @param db
	 * @param rel
	 * @return
	 */
	public static String createRDBMSSchema(SqlDb db, final Relation rel) {
		return db.getSqlTranslator().createSchema(rel.getDbSchema());
	}
	
	public static List<String> repairIndex(SqlDb db, final Relation rel, final String suffix, final Table t,
			boolean withNoLogging) {
		ArrayList<String> ret = new ArrayList<String>();

		String inxName = rel.getPreferredIndexName(suffix);
		
		Index inx = t.getIndex(inxName);
		boolean createIndex = true;
		
		List<RelationField> indexes =  rel.getPrimaryKey().getFields();
		
		// Does index already exist?
		if (inx != null) {
			ret.add(((SqlDb)db).dropSQLIndexCode(suffix, rel));
			
			// Does it have the appropriate keys?
			int i = 0;
			createIndex = false;
			for (IndexColumn ic : inx.getColumns()) {
				if (!(ic.getName().equals(indexes.get(i).getName().toUpperCase()))) {
					createIndex = true;
				}
				i++;
			}
		}
		
		if (createIndex)
			ret.addAll(((SqlDb)db).createSQLIndexCode(suffix, rel, indexes, false, !withNoLogging));
		
		return ret;
	}

	/**
	 * Repairs a table that doesn't match our Relation definition, by adding and removing columns
	 * 
	 * @param db
	 * @param rel
	 * @param suffix
	 * @param t
	 * @param withNoLogging
	 * @return
	 */
	public static List<String> repairDbTable(SqlDb db, final Relation rel, final String suffix, final Table t,
				boolean withNoLogging) {
			ArrayList<String> ret = new ArrayList<String>();
			
			List<ISqlStatementGen.AttribSpec> attribList = new ArrayList<ISqlStatementGen.AttribSpec>();
			
			List<RelationField> fields = rel.getFields();
			
			// Populate items to add, i.e., missing columns
			for (RelationField f : fields) {
				String nam = f.getName().toUpperCase();
				
				if (t.getColumn(nam) == null) {
					ISqlStatementGen.AttribSpec spec = new ISqlStatementGen.AttribSpec ();
					spec.attribName = nam;
					spec.attribType = f.getSQLTypeName();//f.getSQLType();
					spec.attribDefault = f.getDefaultValueAsString();
					spec.isNullable = false;
					
					attribList.add(spec);
				}
			}

			if (attribList.size() > 0) {
				ret.add(((SqlDb)db).getSqlTranslator().addAttributeList(rel.getQualifiedName(suffix), attribList));
				ret.add(db.getSqlTranslator().reorg(rel.getQualifiedName(suffix)));
				ret.add(db.getSqlTranslator().enableConstraints(rel.getQualifiedName(suffix)));
			}
			
			if (Config.getDropExtraColumns()) {
				List<String> toRemove = new ArrayList<String>();
				for (Column c : t.getColumns()) {
					// Find using uppercase
					boolean found = false;
					String nam = c.getName();
					for (RelationField f : fields)
						if (nam.equals(f.getName().toUpperCase()))
							found = true;
					
					if (!found) {
						toRemove.add(nam);
					}
				}

				if (toRemove.size() > 0) {
					ret.add(((SqlDb)db).getSqlTranslator().dropAttributeList(rel.getQualifiedName(suffix), toRemove));
					ret.add(db.getSqlTranslator().reorg(rel.getQualifiedName(suffix)));
					ret.add(db.getSqlTranslator().enableConstraints(rel.getQualifiedName(suffix)));
				}
			} else
				throw new RuntimeException("Error: extra columns.  Please validate these can be removed and enable config option dropExtraColumns.");

			/*
			ret.add(((SqlDb)db).dropSQLIndexCode(suffix, rel));
			List<RelationField> indexes =  rel.getPrimaryKey().getFields();
			ret.addAll(((SqlDb)db).createSQLIndexCode(suffix, rel, indexes, false, !withNoLogging));
			*/
			ret.addAll(repairIndex(db, rel, suffix, t, withNoLogging));
			return ret;
		}


	// OLD VERSION FROM SCHEMACONVERTERSTATEMENTSGEN
//	private void createDeltaTables (List<String> statements, 
//			boolean withNoLogging, String database, boolean stratified, String NO_LOGGING, SqlDb db, boolean containsBidirectionalMappings)
//	{
//
//
//		// Modified by zives 12/20:  don't create tables for _NONE.  Instead
//		// update the base table
//		for (Relation rel : _sc.getRelations())
//		{
//			for (AtomType type : AtomType.values())
//			{
//				String relName = rel.getDbRelName();
//				//if((relName.endsWith("_L") || relName.endsWith("_R")) && (type == AtomType.RCH)){
//				if ((rel.isInternalRelation() && (type == AtomType.RCH)) 
//						|| ((!containsBidirectionalMappings) && (type == AtomType.D))
//				)
//					//				if ((rel.getName().endsWith(Relation.LOCAL) && (type == AtomType.RCH)) ||
//					//					(rel.getName().endsWith(Relation.REJECT) && (type != AtomType.NONE)))
//				{
//					//					We don't need to create _RCH tables for idbs
//				}else if(!stratified || type != AtomType.ALLDEL){
//					if (type != AtomType.NONE)
//						relName = relName + "_" + type.toString();
//
//					_log.debug("Delta table " + relName + ": prep statement");
//					relName = relName.replaceAll("_", "\\\\_");
//
//					if (type != AtomType.NONE) {
//						if (_existingTableNames.contains(rel.getFullQualifiedDbId() + "_" + type.toString()))
//							statements.add ("DROP TABLE " + rel.getFullQualifiedDbId() + "_" + type.toString());
//					}
//					// gregkar
//					String tablename;
//					//					if(Config.getTempTables() 
//					//							&& type != AtomType.NONE
//					//							){
//					//						tablename = SqlStatementGen.sessionSchema + "." + rel.getDbRelName();
//					//					} else {
//					tablename = rel.getFullQualifiedDbId();
//					//					}
//
//					List<ISqlColumnDef> cols = newArrayList();
//					if(stratified && 
//							(type == AtomType.DEL || type == AtomType.INS)){
//						if(tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL") || 
//								tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS")){
//
//							cols.add(_sqlFactory.newColumnDef("STRATUM", "INTEGER", "0"));
//						}else{
//							cols.add(_sqlFactory.newColumnDef("STRATUM", "INTEGER", null));
//						}
//					}
//
//					if (type != AtomType.NONE)
//						tablename = tablename + "_" + type.toString();
//
//					if (type != AtomType.NONE) {
//
//						for (int i = 0; i < rel.getFields().size(); i++) {
//							cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName(), 
//									rel.getField(i).getSQLTypeName(), null));
//						}
//						if(rel.hasLabeledNulls()){
//							for (int i = 0; i < rel.getFields().size(); i++) {
//
//								if (!Config.useCompactNulls() || rel.isNullable(i)) {
//									if (tablename.endsWith("_L_INS") || tablename.endsWith("_R_INS") ||
//											tablename.endsWith("_L_DEL") || tablename.endsWith("_R_DEL"))
//										cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT, 
//												"INTEGER", "1"));
//									else
//										cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName() + RelationField.LABELED_NULL_EXT, 
//												"INTEGER", null));
//								}
//							}
//						}
//						// gregkar
//						if (!Config.getTempTables() 
//								|| !(tablename.startsWith(ISqlStatementGen.sessionSchema) // || type != AtomType.NONE
//								)
//						) {
//							System.out.println("* Adding non-temp table " + tablename);
//							statements.addAll(db.getSqlTranslator().createTable(tablename, cols, withNoLogging));
//						} else {
//							System.out.println("* Adding delta temp table " + tablename);
//							statements.addAll(db.getSqlTranslator().createTempTable(tablename, cols));
//						}
//
//					}
//
//					List<ISqlColumnDef> v = newArrayList();
//					//					boolean first = true;
//					for (RelationField fld : rel.getFields()) {
//						boolean fldInKey = rel.getPrimaryKey().getFields().contains(fld);
//						if(fldInKey){
//							v.add(_sqlFactory.newColumnDef(fld.getName(), "", null));
//						}
//					}
//
//					// TODO: remove hack
//					if (!INDEX_ALL_FIELDS) {
//
//						//						TEMPORARY HACK
//						v.add(_sqlFactory.newColumnDef("KID", "", null));
//
//
//					} else {
//						if(rel.hasLabeledNulls()){
//							int inx = 0;
//							for (RelationField fld : rel.getFields())
//							{
//								if (!Config.useCompactNulls() || rel.isNullable(inx))
//									v.add(_sqlFactory.newColumnDef(fld.getName() + RelationField.LABELED_NULL_EXT, "", null));
//								
//								inx++;
//							}
//						}
//
//					}
//
//					String stmt;						
//					// gregkar
//					stmt = db.getSqlTranslator().createIndex(tablename + "_IDX ",
//							tablename, v, true, true);
//
//					statements.add (stmt);
//
//					_log.debug("Delta table " + relName + ": statement done");
//
//				}
//			}
//		}
//	}
}
