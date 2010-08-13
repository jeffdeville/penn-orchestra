package edu.upenn.cis.orchestra.obsolete;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleRelationException;
import edu.upenn.cis.orchestra.datamodel.exceptions.SchemaNotFoundException;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlTableManipulation;

/**
@deprecated
*/
public class CreateAuxiliaryStorageSql extends CreateAuxiliaryStorage {
	public CreateAuxiliaryStorageSql()
	{
	}

	public void createAuxiliaryDbTable (final Relation rel, boolean withNoLogging, IDb db) {
		for (final Atom.AtomType type : Atom.AtomType.values()){
			String suffix = Atom.typeToString(type);
			
			if (type != AtomType.RCH && (!Config.getStratified() || type != AtomType.ALLDEL)
					&& (type != AtomType.D)){
				
				boolean addStratum =  (Config.getStratified() &&
						   (type == AtomType.DEL || type == AtomType.INS));

				createAuxiliaryDbTable(rel, type, withNoLogging, addStratum, db);
			}
		}
	}

	/**
	 * Creates the desired table, optionally with a suffix
	 * @param rel
	 * @param type
	 * @param withNoLogging
	 * @param addStratum
	 * @param db
	 */
	public void createAuxiliaryDbTable (final Relation rel, Atom.AtomType type, boolean withNoLogging, boolean addStratum, IDb db) {
		final List<String> toApply = new ArrayList<String>();
		SqlDb d = (SqlDb)db;
		List<RelationField> indexes =  rel.getPrimaryKey().getFields();

		boolean relExists = false;
		String suffix = Atom.typeToString(type);
		
		Debug.println("Working on table " + rel + suffix);
		
		try {
			relExists = SqlTableManipulation.ensureRelationExists(db, rel, suffix);
		} catch (SchemaNotFoundException e) {
			e.printStackTrace();

			assert (false) : "Missing RDBMS schema / fix not implemented";
			
		} catch (IncompatibleRelationException e) {
			e.printStackTrace();

			assert (false) : "Incompatible RDBMS schema / non-destructive fix not implemented";
			
			System.exit(1);

			// TODO: how do we ensure we avoid losing data???
			// Drop the tables since they have the wrong schema
			toApply.add(d.dropSQLTableCode(suffix, rel));
			
		}
		if (!relExists) {
			toApply.addAll(d.createSQLTableCode( 
					suffix, rel, false, addStratum, false, !withNoLogging));

			toApply.addAll(d.createSQLIndexCode(suffix, rel, indexes, false, !withNoLogging));
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
