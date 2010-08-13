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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleKeysException;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.exchange.RuleFieldMapping;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;


/**
 * Root class for defining the provenance encoding
 * in relations.  Abstract and not SQL-specific.
 * 
 * @author zives, gkarvoun
 *
 */
public abstract class CreateProvenanceStorage {
	/**
	 * Position/relation pair (currently unused)
	 * 
	 * @author zives
	 *
	 */
	public class RelPos {
		int pos;
		AbstractRelation rel;
	}

	/**
	 * Returns a hash table with a field mapping for each mapping rule
	 */

	public static Map<String, List<List<RuleFieldMapping>>> createBasicProvenanceMappings(final List<Rule> rules) throws IncompatibleTypesException  
	{
		HashMap<String, List<List<RuleFieldMapping>>> 
		mappings = new HashMap<String, List<List<RuleFieldMapping>>>();

// Iterate through each rule and create a RuleFieldMapping structure for it
		for(int i = 0; i < rules.size(); i++){
			final Rule rule = rules.get(i);

			List<RuleFieldMapping> fields = new ArrayList<RuleFieldMapping>();
			List<RelationField> indexFields = new ArrayList<RelationField>();
//			getProvenanceFieldMapping(rule, fields, indexFields);
			
//			fields.addAll(rule.getBodyFieldMapping(indexFields, true));
			fields.addAll(rule.getAppropriateRuleFieldMapping(indexFields));
			
			// Get the existing structure with the same relation in the head
			// (i.e., relation we are mapping to) and append this rule
			List<List<RuleFieldMapping>> s = 
				mappings.get(rule.getHead().getRelation().getName());
			if (s == null) {
				s = new ArrayList<List<RuleFieldMapping>>();
				mappings.put(rule.getHead().getRelation().getName(), s);
			}
			s.add(fields);

		}

		return mappings;
	}

	/**
	 * Creates a list of rule field mappings for an outer union schema
	 * 
	 * @param relName
	 * @param mappings
	 * @return
	 * @throws UnsupportedTypeException 
	 */
//	public OuterUnionMapping createOuterUnionMappingAndSchema(String relName,
	/*
	public ProvenanceRelation createOuterUnionMappingAndSchema(String relName,
			List<List<RuleFieldMapping>> mappings) throws UnsupportedTypeException {

		OuterUnionMapping oum = new OuterUnionMapping();
		oum.setName(relName);

		oum.getRFMappings().addAll(mappings);

		//List<RuleFieldMapping> newMapping = new ArrayList<RuleFieldMapping>();

		Set<RuleFieldMapping> alreadyAdded = new HashSet<RuleFieldMapping>();

		//boolean isFirst = true;
		int inx = 0;
		//System.out.println("EXPANDING " + mappings.size() + " Rule-Field Mappings");
		for (List<RuleFieldMapping> rel : mappings) {

			// Scan each mapping in the list and see if it is already in the
			// target schema.  If not, we need to add it.
//			int mapIndex = 0;
			for (RuleFieldMapping rm : rel) {
				//	System.out.println(" " + rm.toString());
				boolean fnd = false;
				OuterUnionColumn orig = null;

				// Look only at the target columns to determine if we should
				// merge
				List<String> tColumns2 = new ArrayList<String>();
				for(RelationField r : rm.trgColumns)
					tColumns2.add(r.toString());
				
				Iterator<String> it = tColumns2.iterator();
				String name = null;
				while (it.hasNext() && !fnd) {
					// See if it is part of the output relation -- if so,
					// then it must already be part of the prov relation
					name = it.next();
					//if (name.startsWith(relName)) {
					// Find the corresponding entry in alreadyAdded.
					// Merge in all of our source columns.
					orig = getMatchingMapping(oum, name);
					fnd = (orig != null);
					//}
				}

//				List<String> allColumns = new ArrayList<String>();
//				List<String> tColumns = new ArrayList<String>();
//				for(RelationField r : rm.srcColumns)
//					allColumns.add(r.toString());
//				
//				for(RelationField r : rm.trgColumns)
//					tColumns.add(r.toString());
				
//				If it isn't in the existing target columns, then add a new column
				if (!fnd || rm.trgColumns.size() == 0 || rm.srcColumns.size() == 0) {
					
//					allColumns.addAll(rm.srcColumns);
					
					if (rm.srcColumns.size() == 0 && rm.srcArg == null){
						Debug.println("NULL FIELD?");
//						rm.srcColumns.add(OuterUnionColumn.ORIGINAL_NULL);
						rm.srcColumns.add(new RelationField(OuterUnionColumn.ORIGINAL_NULL, "", new StringType(true, false, 1)));
					}
					
					orig = new OuterUnionColumn(rm.outputField, rm.isIndex, inx,
							rm.srcColumns, rm.trgColumns, rm.srcArg);
					
					oum.getColumns().add(orig);
					alreadyAdded.add(rm);
					assert(orig.getSourceColumns().get(inx) == rm.srcColumns);
					assert(orig.getDistinguishedColumns().get(inx) == rm.trgColumns);
					assert(orig.getSourceArgs().get(inx) == rm.srcArg);
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
					
//					for (int temp = 0; temp < mapIndex - 1; temp++) {
//						orig.getSourceColumns().add(null);
//						orig.getSourceVariables().add(null);
//					}
//					orig.getSourceVariables().add(rm.srcVar);
				} else {
//					allColumns.addAll(rm.srcColumns);

					if (rm.srcColumns.size() == 0 && rm.srcArg == null){
						Debug.println("NULL FIELD?");
//						rm.srcColumns.add(OuterUnionColumn.ORIGINAL_NULL);
						rm.srcColumns.add(new RelationField(OuterUnionColumn.ORIGINAL_NULL, "", new StringType(true, false, 1)));
					}

					// The next line should have no effect since the col is already
					// there:
					//   orig.getDistinguishedColumns().add(rm.trgColumns);
					if (inx < orig.getSourceArgs().size()) {
						orig.getSourceArgs().set(inx, rm.srcArg);
						if (orig.getSourceColumns().get(inx) == null)
							orig.getSourceColumns().set(inx, rm.srcColumns);//rm.srcColumns);
						else
							orig.getSourceColumns().get(inx).addAll(rm.srcColumns);//rm.srcColumns);
						if (orig.getDistinguishedColumns().get(inx) == null)
							orig.getDistinguishedColumns().set(inx, rm.trgColumns);
						else
							orig.getDistinguishedColumns().get(inx).addAll(rm.trgColumns);
					} else {
						//for (int temp = orig.getSourceVariables().size(); temp < mapIndex; temp++)
						//	orig.getSourceVariables().add(null);
						orig.getSourceArgs().add(rm.srcArg);
						orig.getSourceColumns().add(rm.srcColumns);//rm.srcColumns);
						orig.getDistinguishedColumns().add(rm.trgColumns);
					}
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
				}
				// Even the columns all out by padding with nulls

				for (OuterUnionColumn o : oum.getColumns()) {
					if (o.getDistinguishedColumns().size() <= inx)
						o.getDistinguishedColumns().add(null);
					if (o.getSourceColumns().size() <= inx)
						o.getSourceColumns().add(null);
					if (o.getSourceArgs().size() <= inx)
						o.getSourceArgs().add(null);
				}
				assert(orig.getSourceColumns().size() == inx + 1);
				assert(orig.getSourceArgs().size() == inx + 1);
				assert(orig.getDistinguishedColumns().size() == inx + 1);

				// TODO: condense based on overlap???
				//		mapIndex++;
			}
			//alreadyAdded.addAll(rel);
			inx++;
		}

		return createOuterUnionRelation(relName, oum);	
	}
	*/

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


	public abstract void createOuterUnionDbTable(ProvenanceRelation rel,  
			boolean withLogging, IDb Db); 

	public abstract void createProvenanceDbTable (final Relation rel, boolean withNoLogging, IDb db, boolean containsBidirectionalMappings);

	/**
	 * Divide the relation attributes into equivalence classes, based on what
	 * got equated.
	 * 
	 * @param r
	 * @return
	 * @deprecated
	 */
	public Map<RelPos,Set<RelPos>> getEquivalenceClasses(Rule r) {
		Map<RelPos,Set<RelPos>> ret = new HashMap<RelPos,Set<RelPos>>();

		int siz = r.getHead().getValues().size();
		RelPos rp = new RelPos();
		rp.rel = r.getHead().getRelation();
		for (int i = 0; i < siz; i++) {
			//ScMappingAtomValue v = r.getHead().getValues().get(i);
			// TODO:  finish this
		}

		for (Atom a : r.getBody()) {
			rp = new RelPos();
			rp.rel = a.getRelation();
			siz = a.getValues().size();
			for (int i = 0; i < siz; i++) {
				//ScMappingAtomValue v = a.getValues().get(i);

				// TODO: finish this
			}
		}

		return ret;
	}



}
