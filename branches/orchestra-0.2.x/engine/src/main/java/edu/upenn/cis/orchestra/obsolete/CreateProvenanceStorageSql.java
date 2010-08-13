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
package edu.upenn.cis.orchestra.obsolete;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleRelationException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationUpdateException;
import edu.upenn.cis.orchestra.datamodel.exceptions.SchemaNotFoundException;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlTableManipulation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;

/**
 * 
 * @author gkarvoun
 * @deprecated
 *
 */
//WARNING: This can only work if everything is in the same db instance!!!!
public class CreateProvenanceStorageSql extends CreateProvenanceStorage {
	
	//private JdbcTemplate _jt;
	private boolean _indexAll;
	
	public CreateProvenanceStorageSql()//DataSource ds)
	{
		_indexAll = Config.getIndexAllFields();
	}
	
	/*
	public void collectStatistics (Connection cn, OrchestraSystem syst) throws SQLException
	{
		
		//cn.setAutoCommit(true);
		if (SqlDb.isDB2())
			for (Peer p : syst.getPeers())
				for (Schema sc : p.getSchemas())
					for (TableSchema rel : sc.getRelations())
					{
						collectStatistics (cn, rel.getFullQualifiedDbId());
						for (ScMappingAtom.AtomType type : ScMappingAtom.AtomType.values())
							collectStatistics (cn, 
									(rel.getDbSchema()!=null?rel.getDbSchema() + ".":"")
									+ rel.getDbRelName() + "_" + type.toString()
								);
					}
		//cn.setAutoCommit(false);
	}
	
	private void collectStatistics (Connection cn, String tabName) throws SQLException
	{
//		DatalogEngine.preparedStmtCnt++;
//		PreparedStatement stmt = cn.prepareStatement("RUNSTATS ON TABLE " + DatalogEngine.DBUSER + "." + tabName + " ON ALL COLUMNS ALLOW WRITE ACCESS");
//		stmt.execute();

		_db.evaluate("RUNSTATS ON TABLE " + SqlDb.DBUSER + "." + tabName + " ON ALL COLUMNS ALLOW WRITE ACCESS");
	}*/

	public void createOuterUnionDbTable(final ProvenanceRelation rel, 
			boolean withLogging,
			IDb db) { 
		final List<String> toApply = new ArrayList<String>();
		//final List<ScField> fields = rel.getFields();
//		Set<Integer> indexes = new HashSet<Integer>();
		List<RelationField> indexes =  rel.getPrimaryKey().getFields();
		SqlDb d = (SqlDb)db;
					
		rel.markFinished();
		
		for (final AtomType type : AtomType.values()){
			String suffix = Atom.typeToString(type);
			
			if((type != AtomType.RCH) && (!Config.getStratified() || type != AtomType.ALLDEL)){
				if(!suffix.equals("")){
					suffix = "_" + suffix;
				}
				
				if(Config.getStratified() &&
						   (type == AtomType.DEL || type == AtomType.INS)){
//							Add "MRULE"
//							toApply.addAll(d.createSQLTableCode(rel.getName(), 
//									suffix, rel, true, true, true, false, withLogging));
//							MRULE added already
							toApply.addAll(d.createSQLTableCode( 
									suffix, rel, false, true, false, withLogging));

							//toApply.addAll(createProvenanceTable ("M" + ruleIndice + suffix, 
							//		relLoc, buffDel, indexes, allVars.size()));
							
//							toApply.addAll(d.createSQLIndexCode(rel.getName(), suffix, rel, indexes, true, true, withLogging));
//							Don't normalize attr names
							toApply.addAll(d.createSQLIndexCode(suffix, rel, indexes, false, withLogging));
						}else{
//							Add MRULE
//							toApply.addAll(d.createSQLTableCode(rel.getName(), 
//									suffix, rel, true, false, true, false, withLogging));
//							MRULE added already
							toApply.addAll(d.createSQLTableCode( 
									suffix, rel, false, false, false, withLogging));

//							toApply.addAll(d.createSQLIndexCode(rel.getName(), suffix, rel, indexes, true, true, withLogging));
//							Don't normalize attr names
							toApply.addAll(d.createSQLIndexCode(suffix, rel, indexes, false, withLogging));

							//toApply.addAll(createProvenanceTable ("M" + ruleIndice + suffix, 
							//		relLoc, buffStat, indexes, allVars.size()));
						}

				
			}
		}
				
		for (final String s: toApply) {
			Debug.println(s);
		}

		if (Config.getApply()) {
			for (final String s: toApply) {
				((SqlDb)db).evaluate(s);
			}
		}
		
//		return toApply;
	}
	

//	/**
//	 * @deprecated
//	 * @param rel
//	 * @param oum
//	 * @param withLogging
//	 * @param create
//	 * @return
//	 */
//	public List<String> OLDcreateOuterUnionStatement(final Relation rel, 
//			final OuterUnionMapping oum,
//			boolean withLogging, boolean create) { 
//		final List<String> toApply = new ArrayList<String>();
//		//final List<ScField> fields = rel.getFields();
//		Set<Integer> indexes = new HashSet<Integer>();
//		
//		indexes = createOuterUnionKeys(rel, oum, withLogging, create);
//			
//		final List<String> srcColumns = new ArrayList<String>();
//		//final List<String> srcRelations = new ArrayList<String>();
//		final List<String> srcTypes = new ArrayList<String>();
//		for (final OuterUnionColumn m: oum.getColumns()) { //RuleFieldMapping m : fm) {
//			for (final List<String> l : m.getSourceColumns()) {
//				if (l != null) {
//					// Find any source column
//					for (final String src : l) {
//						final String relN = src.substring(0, src.indexOf('.'));
//						final String colN = src.substring(src.indexOf('.') + 1);
//						
//						/*
//						if (!srcRelations.contains(relN))
//							srcRelations.add(relN);
//							*/
//						
//						srcColumns.add(src);
//						
//						for (Schema sch : _db.getSchemas()) {
//							for (AbstractRelation r : sch.getRelations()) {
//								if (r.getName().equals(relN)) {
//									//System.out.println(colN + " in " + r.getFields());
//									if (r.getField(colN) != null) {
//										srcTypes.add(r.getField(colN).getSQLTypeName());
//										break;
//									}
//								}
//							}
//						}
//						break;
//					}
//					break;
//				}
//			}
//			
//		}
//		
//		if (srcTypes.size() != srcColumns.size())
//			throw new RuntimeException("Unable to find types for all columns:\n" + srcColumns + "\n" + srcTypes);
//
//		// If the table does not exist already, create it
//		// We don't have time to make it clean now... just create the table and ignore the exception if 
//		// it already exists.
//		final StringBuffer buffStat = new StringBuffer ();
//		final StringBuffer buffDel = new StringBuffer ();
//		//buffStat.append(" AS (SELECT CAST (0 AS INTEGER) AS MRULE");
//		buffStat.append(" (");
//		buffDel.append(" (");
//		if(Config.getStratified())
//			buffDel.append("STRATUM INTEGER, ");
//		buffStat.append("MRULE INTEGER");
//		buffDel.append("MRULE INTEGER");
//		
//		boolean first = false;//true;
//		int indCol = 0;
//		for (final String fld : srcColumns)
//		{
//			//buffStat.append((first?"":",") + fld + " AS C" + indCol);
//			buffStat.append((first?"":",") + "C" + indCol + " " + srcTypes.get(indCol));
//			buffDel.append((first?"":",") + "C" + indCol + " " + srcTypes.get(indCol));
//			indCol++;
//			first = false;
//		}
//		indCol = 0;
//		for (final String fld : srcColumns)
//		{
//			//buffStat.append("," + fld + ModelConstants.LABEL_NULL_EXT + " AS C" + indCol + ModelConstants.LABEL_NULL_EXT) ;
//			buffStat.append(",C" + indCol + ModelConstants.LABEL_NULL_EXT + " INTEGER");// + srcTypes.get(indCol));
//			buffDel.append(",C" + indCol + ModelConstants.LABEL_NULL_EXT + " INTEGER");// + srcTypes.get(indCol));
//			indCol++;
//			first = false;
//		}
//
//		buffStat.append(")");
//		buffDel.append(")");
//		
//		if (Config.isDB2() || Config.isOracle()) {
//			
//	//		buffStat.append(" WITH NO DATA");
//			if (!withLogging){
//				buffStat.append(Config.getLoggingMsg());
//				buffDel.append(Config.getLoggingMsg());
//			}
//		}
//		
//		final Relation relLoc = rel;
//				
//		for (final AtomType type : AtomType.values()){
//			String suffix = Atom.typeToString(type);
//			
//			if((type != AtomType.RCH) && (!Config.getStratified() || type != AtomType.ALLDEL)){
//				if(!suffix.equals("")){
//					suffix = "_" + suffix;
//				}
//				
//				if(Config.getStratified() && 
//				   (type == AtomType.DEL || type == AtomType.INS)){
//					toApply.addAll(createProvenanceTable (rel.getName() + suffix, 
//							relLoc, buffDel, indexes, srcColumns.size()));
//				}else{
//					toApply.addAll(createProvenanceTable (rel.getName() + suffix, 
//							relLoc, buffStat, indexes, srcColumns.size()));
//				}
//				
//			}
//		}
//				
//		if(create){
//			for (final String s: toApply) {
//				Debug.println(s);
//			}
//		
//			if (Config.getApply()) {
//				for (final String s: toApply) {
//					((SqlDb)_db).evaluate(s);
//				}
//			}
//		}
//		
//		return toApply;
//	}
	
	public void createProvenanceDbTable (final Relation rel, boolean withLogging, IDb db, boolean containsBidirectionalMappings)
	{
		// TODO: replace with createAuxiliaryDbTable
		
		final List<String> toApply = new ArrayList<String>();
		SqlDb d = (SqlDb)db;
		List<RelationField> indexes =  rel.getPrimaryKey().getFields();

		boolean relExists = false;
		
		try {
			relExists = SqlTableManipulation.ensureRelationExists(db, rel);
		} catch (SchemaNotFoundException e) {
			e.printStackTrace();

			assert (false) : "Missing RDBMS schema / fix not implemented";
			
		} catch (IncompatibleRelationException e) {
			e.printStackTrace();

			assert (false) : "Incompatible RDBMS schema / non-destructive fix not implemented";
			
			System.exit(1);

			// TODO: how do we ensure we avoid losing data???
			// Drop the tables since they have the wrong schema
			for (final Atom.AtomType type : Atom.AtomType.values()){
				String suffix = Atom.typeToString(type);
				if(type != AtomType.RCH && (!Config.getStratified() || type != AtomType.ALLDEL)
						&& (containsBidirectionalMappings || type != AtomType.D)
						){
					if(!suffix.equals("")){
						suffix = "_" + suffix;
					}
					toApply.add(d.dropSQLTableCode(suffix, rel));
				}
			}
			
		}
		if (!relExists)
		{
			for (final Atom.AtomType type : Atom.AtomType.values()){
				String suffix = Atom.typeToString(type);
			
				if(type != AtomType.RCH && (!Config.getStratified() || type != AtomType.ALLDEL)
						&& (containsBidirectionalMappings || type != AtomType.D)
						){
					if(!suffix.equals("")){
						suffix = "_" + suffix;
					}

					if(Config.getStratified() &&
					   (type == AtomType.DEL || type == AtomType.INS)){
						toApply.addAll(d.createSQLTableCode( 
								suffix, rel, false, true, false, withLogging));
						//toApply.addAll(createProvenanceTable ("M" + ruleIndice + suffix, 
						//		relLoc, buffDel, indexes, allVars.size()));
						toApply.addAll(d.createSQLIndexCode(suffix, rel, indexes, false, withLogging));
					}else{
						toApply.addAll(d.createSQLTableCode( 
								suffix, rel, false, false, false, withLogging));
						//toApply.addAll(createProvenanceTable ("M" + ruleIndice + suffix, 
						//		relLoc, buffStat, indexes, allVars.size()));
						toApply.addAll(d.createSQLIndexCode(suffix, rel, indexes, false, withLogging));
					}
					
				}
			}
		} 

		for (final String s: toApply) {
			Debug.println(s);
		}

		if (Config.getApply()) {
			for (final String s: toApply) {
				((SqlDb)db).evaluate(s);
			}
		}
	}
}
