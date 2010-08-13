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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomSkolem;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.ClobType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.dbms.BuiltinFunctions;
import edu.upenn.cis.orchestra.dbms.IRuleCodeGen;
import edu.upenn.cis.orchestra.dbms.UDFunctions;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlDelete;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlFromItem;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.ISqlSelectItem;
import edu.upenn.cis.orchestra.sql.ISqlSimpleExpression;
import edu.upenn.cis.orchestra.sql.SqlFactories;


/**
 * A wrapper that takes a rule and generates updates or queries
 * 
 * @author zives, gkarvoun
 *
 */
public class RuleSqlGen implements IRuleCodeGen {
	protected Map<String,String> m_varmap;
	protected Map<String,Type> m_vartype;
	protected Rule m_rule;
	protected boolean m_stratified;
	protected boolean m_external;
	protected ISqlStatementGen m_sqlString;

	protected Map<String,ISqlExp> _whereExpressions;
	protected Set<ISqlExp> _whereRoots;
	private final ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();
	// TO DO:  add info about the Schemas for each item.
	// If the Schemas have no labeled nulls, we shouldn't add them.
	// If the rule atom doesn't match the Schema, then we should throw an exception.

	public RuleSqlGen(Rule rule, Map<String,Schema> builtIns) {
		this(rule, builtIns, Config.getStratified(), false);
	}
	
	public RuleSqlGen(Rule rule, Map<String,Schema> builtIns, boolean stratified, boolean external) {
		m_rule = rule;
		m_stratified = stratified;
		m_external = external;
		BuiltinFunctions.setBuiltins(builtIns);

		m_sqlString = SqlStatementGenFactory.createStatementGenerator();
	}

	public ISqlSelect toQuery(int curIterCnt) 
	{
		ISqlSelect q =  _sqlFactory.newSelect();
		_whereExpressions = newHashMap();
		_whereRoots = newHashSet();
		m_varmap = buildVarMap(m_rule);
		m_vartype = buildTypeMap(m_rule);

		List<ISqlFromItem> fr = buildFrom();
		List<ISqlSelectItem> sel = buildSelect(curIterCnt, fr);
	
		q.addSelectClause(sel);
		q.addFromClause(fr);
		q.addWhere(buildWhere(curIterCnt));
		
		//System.out.println(q.toString());
		return q;
	}    

	public ISqlSelect toQuery() 
	{
		return toQuery(0);
	}

	public ISqlInsert toInsert()
	{
		return toInsert(0);
	}

	public ISqlInsert toInsert(int curIterCnt)
	{
		Atom head = m_rule.getHead();
		ISqlInsert ins;
		if (m_rule.getBody().size() == 0) {
			// INSERT INTO statement using VALUES
			Relation rel = head.getRelation();
			ins = _sqlFactory.newInsert(rel.toAtomString(head.getType()));
			List<AtomArgument> values = head.getValues();
			if(values.size() > 1){
				ISqlExpression comma = _sqlFactory.newExpression(ISqlExpression.Code.COMMA);
				for (int j = 0; j < values.size(); j++) {
					String str = values.get(j).toString();
					ISqlConstant.Type type = getSqlConstantType(head.getRelation().getField(j));
					ISqlConstant c = _sqlFactory.newConstant(str, type);
					comma.addOperand(c);
				}
				ins.addValueSpec(comma);
			}else if (values.size() == 1){
				ISqlExpression single = _sqlFactory.newExpression(ISqlExpression.Code.COMMA);
				String str = values.get(0).toString();
				ISqlConstant.Type type = getSqlConstantType(head.getRelation().getField(0));
				ISqlConstant c = _sqlFactory.newConstant(str, type);
				single.addOperand(c);
				ins.addValueSpec(single);
			}
		} else {
			List<String> cols = head.getRelation().getFieldsInList();
			
			if (Config.getStratified() && (head.getType().equals(AtomType.INS) || head.getType().equals(AtomType.DEL))) {
				cols.add("STRATUM");
			}
			
			// INSERT INTO statement with a subquery
			ins = _sqlFactory.newInsert(head.toString3(), cols);
			if (Config.getSetSemantics()) {
				// Enforce set semantics
				Atom last = m_rule.getBody().get(m_rule.getBody().size()-1);

				if(m_rule.getHead().getRelation().equals(last.getRelation()) && 
						m_rule.getHead().getType().equals(last.getType()) && 
						last.isNeg()){
					// already contains "not exists"
					Debug.println("NOT EXISTS ALREADY THERE");
				}else{
					Atom notExists = m_rule.getHead().deepCopy();
					notExists.negate();
					if(notExists.getType() == AtomType.DEL || notExists.getType() == AtomType.INS)
						notExists.setAllStrata();
					m_rule.getBody().add(notExists);
				}
			}
			ISqlSelect q1 = this.toQuery(curIterCnt);
			if(Config.getSetSemantics() && this.m_rule.isDistinct())
				q1.setDistinct(true);

//			HACK!!!
			if(!Config.getNotExists()){
				Atom lastAtom = m_rule.getBody().get(m_rule.getBody().size()-1);
			if(lastAtom.isNeg() && lastAtom.getRelation().equals(m_rule.getHead().getRelation())
					&& lastAtom.getType().equals(m_rule.getHead().getType())){
				ISqlSelect exceptQuery = _sqlFactory.newSelect(_sqlFactory.newSelectItem("*"), 
						_sqlFactory.newFromItem(lastAtom.toString3()),
						null);
				q1.addSet(_sqlFactory.newExpression(ISqlExpression.Code.EXCEPT, exceptQuery));
			}
		}
		ins.addValueSpec(q1);
	}
	return ins;
}

	public ISqlDelete toDelete()
	{
		Atom head = m_rule.getHead();
		ISqlDelete del = _sqlFactory.newSqlDelete(head.toString3());
		if (m_rule.getBody().size() == 0) {
			AbstractRelation rel = head.getRelation();
			ISqlExp exp=null;
			List<AtomArgument> values = head.getValues();
			for (int i = 0; i < values.size(); i++) {
				ISqlExpression eq = _sqlFactory.newExpression(ISqlExpression.Code.EQ);
				RelationField f = rel.getField(i);
				ISqlConstant c1 = _sqlFactory.newConstant(f.getName(), ISqlConstant.Type.COLUMNNAME);
				ISqlConstant c2 = _sqlFactory.newConstant(values.get(i).toString(), getSqlConstantType(f));
				eq.addOperand(c1);
				eq.addOperand(c2);
				exp = conjoin(exp, eq);
			}
			del.addWhere(exp);
		} else {
	//		SqlQuery q1 = this.toQuery(curIterCnt);
	//		del.addValueSpec(q1);
			assert(false);	// UNIMPLEMENTED
			return null;
		}
	
		return del;
	}

	public List<String> getCode(UPDATE_TYPE u, int curIterCnt) {
		List<String> ret = new ArrayList<String>();
		
		if (u == UPDATE_TYPE.CLEAR_AND_COPY){
			Atom oldA = m_rule.getHead();
			Atom newA = m_rule.getBody().get(0);
	
			ret.addAll(m_sqlString.clearAndCopy(oldA.toString3(), newA.toString3()));
	//		}else if (u == UPDATE_TYPE.DELETE_FROM_HEAD){
		}else if ((u == UPDATE_TYPE.DELETE_FROM_HEAD) 
	//			&& (_r.getHead().getRelation().equals(_r.getBody().get(0).getRelation())) &&
	//			(_r.getHead().getType().equals(_r.getBody().get(0).getType()))
		) {
			Atom rel = m_rule.getHead();
			Atom del = m_rule.getBody().get(1);
	
			//	String delete = new String("DELETE FROM " + rel.toString3() + " WHERE (SELECT * FROM " + del.toString3() + ")");
			//db2 => DELETE FROM M0_OLD WHERE EXISTS (SELECT * FROM M0_DEL M WHERE C0 = M.C0 a
			//		nd C1 = M.C1)
			//ret.add(delete);
	
			ISqlSelect n =  _sqlFactory.newSelect();
			m_varmap = buildVarMapForAtom(rel, 0, null, true);
			HashMap<String,String> atomMap = buildVarMapForAtom(del, 1, null, true);
			n.addSelectClause(buildSelectForAtom(del, atomMap, false, false, curIterCnt));
			List<ISqlFromItem> v = newArrayList();
			v.add(buildFromItem(del, 1));
			n.addFromClause(v);
	//		n.addWhere(OLDbuildWhereForAtom(del, 1, m_varmap, null, curIterCnt));
			n.addWhere(buildWhereForAtom(del, 1, m_varmap, null, curIterCnt));
	
			ISqlExpression expr = _sqlFactory.newExpression(ISqlExpression.Code.EXISTS, n);
	
			ISqlDelete d = _sqlFactory.newDelete(rel.toString3(), "R0");
			d.addWhere(expr);
			ret.add(d.toString());
	
	//		String delete = new String("DELETE FROM " + rel.toString3() + " R0 WHERE " + expr.toString());
	//		ret.add(delete);
		}else{ // DELETE FROM HEAD where bodyat0 <> head goes here, too
			if (m_rule.getHead().isNeg() || m_rule.getBody().size() == 0){
				ISqlDelete del = _sqlFactory.newSqlDelete(m_rule.getHead().toString3());
				ret.add(del.toString());
			}else{	
				ret.add(toInsert(curIterCnt).toString());
			}
		}
		//Integer retsize = new Integer(ret.size());
		//Debug.println(retsize.toString());
		return ret;
	}    
	
	/**
	 * Given an existing set of SQL statements, see if
	 * we can merge with any, and return the combined list.
	 *  
	 */
	public List<String> getCode(List<String> existing, UPDATE_TYPE u, int curIterCnt) {
		List<String> newItem = getCode(u, curIterCnt);
		List<String> ret = new ArrayList<String>();
	
		ret.addAll(existing);
	
		int count = 0;
	
		for (String n : newItem) {
			if((Config.getUnion()) && (n.toLowerCase().startsWith("insert into "))) {
				final int start = 12;
				int end = n.indexOf(' ', 12);
	
				String table = n.substring(start, end);
	
				boolean found = false;
				for (int i = 0; i < ret.size(); i++) {
					String n2 = ret.get(i);
	
					if (n2.toLowerCase().startsWith("insert into ")) {
						int end2 = n2.indexOf(' ', 12);
	
						String table2 = n2.substring(start, end2);
	
						found = (table.equals(table2));// && false;
	
						//int length = n2.length() + n.length();
	//					Debug.println("LENGTH: " + length);
						// We have found an insertion into the same table 
						if (found && !(n.equals(n2)) && (n2.length() + n.length() < 10000)) {
							count++;
							ret.set(i, n2 + "\n UNION " + n.substring(end + 1));
							break;
						}else{
							found = false;
						}
					}
				}
				if (!found)
					ret.add(n);
			} else
				ret.add(n);
		}
	
	
		return ret;
	}
	
	
	protected HashMap<String,String> buildVarMapForAtom(Atom a, int i, 
			HashMap<String,String> oldmap, boolean buildNew) {
		HashMap<String,String> newmap = new HashMap<String,String>();
		HashMap<String,String> map;
	
		if(buildNew){
			map = newmap;
		}else{
			map = oldmap;
		}
	
		for (int j = 0; j < a.getValues().size(); j++) 
		{
			AtomArgument val = a.getValues().get(j);
	
			if (val instanceof AtomVariable)
			{
				if (!map.containsKey(val.toString())) {
					String value;
					if (val.toString().equals("-") || val.toString().equals("_")) {
						value = "NULL";
					} else if(i >= 0){
						/*
						// TRUST column...
						if (j >= a.getRelation().getFields().size() && a.getRelation().isInternalRelation()) {
							AtomAnnotation ann = AtomAnnotationFactory.createPeerTrustAnnotation("P1", "temp");
							
							value = ann.getDefaultTrustValue();
							
						} else
						*/
							value = "R" + i + "." + a.getRelation().getField(j).getName();
					}else{
						value = a.getRelation().getField(j).getName();
					}
					map.put(val.toString(), value);
				}
			}
		}
		return map;
	}
	
	protected HashMap<String,String> buildVarMap(Rule r) throws RuntimeException {
		HashMap<String,String> varmap = new HashMap<String,String>();
		List<Atom> atomsToProcess = new ArrayList<Atom>();
		HashMap<Atom,Integer> inx = new HashMap<Atom,Integer>();
		for (int i = 0; i < r.getBody().size(); i++) {
			Atom a = r.getBody().get(i);
			if(!a.isNeg() && !a.isSkolem()) {
				atomsToProcess.add(a);
				inx.put(a, i);
			}
		}
	
		while (atomsToProcess.size() > 0 ){
	
			int x = atomsToProcess.size();
			boolean didAtLeastOneThing = false,didSomething = false;
	
			for (int i = 0; i < x; i++) {
				Atom a = atomsToProcess.get(i);//r.getBody().get(i);
	
				if (BuiltinFunctions.isBuiltIn(a.getSchema().getSchemaId(), a.getRelation().getName())) {
					String val = BuiltinFunctions.evaluateBuiltIn(a, varmap, _whereExpressions, _whereRoots, r.getBody());
					if (val.length() != 0) {
						didAtLeastOneThing = true;
						didSomething = true;
						if (varmap.get(a.getValues().get(0).toString()) == null)
							varmap.put(a.getValues().get(0).toString(), val);
					}
				} else {
					didAtLeastOneThing = true;
					didSomething = true;
					//	Debug.println("Var map for " + a.toString() + File.separator + a.getRelation().toString());
					buildVarMapForAtom(a, inx.get(a), varmap, false);
				}
				if (didSomething) {
					atomsToProcess.remove(i);
					x--;
					i--;
				}
				didSomething = false;
			}
	
			// Test whether we were able to process this atom -- if not,
			// add to pending list
			if (!didAtLeastOneThing && atomsToProcess.size() > 0)
				throw new RuntimeException("Unable to find all variables in " + r);
		}
	
		//Debug.println(varmap);
		return varmap;
	}    
	
	/**
	 * Creates a map from each variable to its type
	 * 
	 * @param r
	 * @return
	 * @throws RuntimeException
	 */
	protected Map<String,Type> buildTypeMap(Rule r) throws RuntimeException {
		HashMap<String,Type> typemap = new HashMap<String,Type>();
	
		for (int i = 0; i < r.getBody().size(); i++) {
			Atom a = r.getBody().get(i);
	
			for (AtomArgument arg : a.getValues()) {
				if (arg instanceof AtomVariable) {
					Type t = arg.getType();
					if (typemap.get(arg.toString()) == null)
						typemap.put(arg.toString(), t);
				}
			}
		}
	
		return typemap;
	}    
	
	protected ISqlFromItem buildFromItem(Atom a, int i){
		//SqlFromItem f = new SqlFromItem(a.getRelation().getFullQualifiedDbId());
		ISqlFromItem f = _sqlFactory.newFromItem(a.toString3());
		if(i >= 0)
			f.setAlias("R" + i);
		return f;
	}
	
	protected ISqlFromItem buildFromItem(ISqlFromItem.Join type, Atom a, Atom b, int leftPos, int rightPos, ISqlExp cond)
	{
		ISqlFromItem f1 = buildFromItem(a, leftPos);
		ISqlFromItem f2 = buildFromItem(b, rightPos);
		ISqlFromItem f3 = _sqlFactory.newFromItem(type, f1,f2, cond);
	
		return f3;
	}
	/**
	 * Create FROM clauses for all atoms that correspond to relations.
	 * This excludes negated atoms, Skolemized atoms, and built-in atoms.
	 * 
	 * @return
	 */
	protected List<ISqlFromItem> buildFrom() {
		List<ISqlFromItem> vf = newArrayList();
		int i = 0;
	
		for (Atom a : m_rule.getBody()) {
			if(!a.isNeg() && !a.isSkolem() && !BuiltinFunctions.isBuiltIn(a.getSchema().getSchemaId(), a.getRelation().getName()) 
					&& (!isDependentOnUDF(a.getRelation()))){
				vf.add( buildFromItem(a, getPositionOfAtom(a)));}
			// Added by Marie J., for outer joins when doing UDFS on pairs 
			else if(UDFunctions.isUDF(a.getRelation().getName()))
			{
				List<Atom> depAtoms = getDepAtomsFor(a);
				// For now, assume only two (for record-linkages).
				Atom leftAtom = depAtoms.get(0), rightAtom = depAtoms.get(1); 
				ISqlExp expr = _whereExpressions.get(a.getValues().get(0).toString());
				expr = getWhereRootContaining(expr);
				if(a.getSchema().getSchemaId().equals("EQUALITYUDFSL"))
					vf.add(buildFromItem(ISqlFromItem.Join.LEFTOUTERJOIN,leftAtom, rightAtom,getPositionOfAtom(leftAtom),getPositionOfAtom(rightAtom),expr));
				else
					vf.add(buildFromItem(ISqlFromItem.Join.RIGHTOUTERJOIN,leftAtom, rightAtom,getPositionOfAtom(leftAtom),getPositionOfAtom(rightAtom),expr));
				_whereExpressions.remove(a.getValues().get(0).toString());
				_whereRoots.remove(expr);
	
			}else{
				i++;
			}
		}
		return vf;
	}
	private boolean isDependentOnUDF(Relation r)
	{
		String s = UDFunctions.isDependentRelation(r);
		if(s==null)
			return false;
		else
		{
			for(Atom a: m_rule.getBody()){
				if(a.getRelation().getName().equals(s))
					return true;
			}
		}
		return false;
	}
	private int getPositionOfAtom(Atom a){
		return m_rule.getBody().indexOf(a);
	}
	private ISqlExp getWhereRootContaining(ISqlExp expr){
		for(ISqlExp s: _whereRoots){
			if(s.getOperands().contains(expr))
				return s;
		}
		return null;
	}
	
	
	private List<Atom> getDepAtomsFor(Atom a){
	
		List<Atom> deps = new ArrayList<Atom>();
		Relation r = a.getRelation();
		List<Relation> depRel = UDFunctions.getDependenciesFor(r.getName());
		for(Atom aa:m_rule.getBody()){
			if(a.equals(aa))
				continue;
			if(depRel.contains(aa.getRelation()))
				deps.add(aa);
		}
		return deps;
	}
	protected ISqlExpression hack(ISqlConstant c1, ISqlConstant c2){
		return(_sqlFactory.newExpression(ISqlExpression.Code.OR,
				_sqlFactory.newExpression(ISqlExpression.Code.AND,
						_sqlFactory.newExpression(ISqlExpression.Code.IS_NULL, c1), 
						_sqlFactory.newExpression(ISqlExpression.Code.IS_NULL, c2)),
						_sqlFactory.newExpression(ISqlExpression.Code.AND,
								_sqlFactory.newExpression(ISqlExpression.Code.IS_NOT_NULL, c1), 
								_sqlFactory.newExpression(ISqlExpression.Code.IS_NOT_NULL, c2))
		)
		);
	}
	
	protected static ISqlConstant.Type getSqlConstantType(RelationField field) {
		String name = field.getSQLTypeName();
		return zType(name);
	}
	
	protected static ISqlConstant.Type zType(String type){
		if(type.contains("char")){
			return ISqlConstant.Type.STRING;
		}else if(type.contains("integer") || type.contains("decimal")){
			return ISqlConstant.Type.NUMBER;
		}else if(type.contains("date")){
			return ISqlConstant.Type.DATE;
		}
	
		return ISqlConstant.Type.UNKNOWN;
	}
	
	protected ISqlExpression stratumCondition(Atom a, int i, int curIterCnt)
	{
		if (m_stratified && Config.getStratified()) { 
	//		This is an "internal" Orchestra delta rule and 
	//		"stratified" (i.e., optimized/non-redundant) evaluation is "activated"
	
			boolean isStratifiedAtom = (a.getType() == AtomType.DEL) || (a.getType() == AtomType.INS);
			if(isStratifiedAtom && !a.allStrata()){
	//			... involving relations that have a "stratum" attribute, that should be treated specially
				ISqlConstant c1;
				if(i >= 0)
					c1 = _sqlFactory.newConstant("R" + i + ".STRATUM", ISqlConstant.Type.COLUMNNAME);
				else
					c1 = _sqlFactory.newConstant("STRATUM", ISqlConstant.Type.COLUMNNAME);
	
				ISqlConstant c2;
				if(Config.getPrepare()){
	//				c2 = new SqlConstant("CAST(? AS INTEGER)", SqlConstant.NUMBER);
					c2 = _sqlFactory.newConstant("?", ISqlConstant.Type.NUMBER);
	//				c2 = new SqlConstant("?", SqlConstant.STRING);
					if(a.getRelationContext().isMapping()){
						m_rule.addPreparedParam(0);
					}else{
						m_rule.addPreparedParam(-1);
					}
	
				}else{
					if(a.getRelationContext().isMapping()){
						c2 = _sqlFactory.newConstant(Integer.toString(curIterCnt), ISqlConstant.Type.NUMBER);
					}else{
						c2 = _sqlFactory.newConstant(Integer.toString(curIterCnt-1), ISqlConstant.Type.NUMBER);
					}
				}
	
				return _sqlFactory.newExpression(ISqlExpression.Code.EQ, c1, c2);
			}
		}
		return null;
	}
	
	protected ISqlExpression isNotNull(RelationField attribute, int pos)
	{
		String l = fullNameForAttr(attribute, pos);
	
		ISqlConstant c3 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
		ISqlConstant c4 = _sqlFactory.newConstant("-1", ISqlConstant.Type.COLUMNNAME);
		ISqlExpression expr = _sqlFactory.newExpression(ISqlExpression.Code.NEQ, c3, c4);
	
		return expr;
	}
	
	protected ISqlExp conditionsForConstant(RelationField attribute, AtomConst c, 
			int pos, boolean comparable, boolean compareNullColumn)
	{
		String l = fullNameForAttr(attribute, pos);
		ISqlConstant c1 = _sqlFactory.newConstant(l, ISqlConstant.Type.COLUMNNAME);
		ISqlExp expr = null;
	
		boolean fakeSkolemConstant = (c.getValue() == null || "-".equals(c.getValue()) || "_".equals(c.getValue()));
		boolean labNullConst = (c.getValue().toString().startsWith("NULL("));
		//		hack for experiments
		//		Use - instead of skolems
		//		Do we need this anymore?
	
		if(labNullConst){
			String skolemColumnValue = m_sqlString.skolemColumnValue(attribute.getSQLTypeName());
			ISqlConstant c2 = _sqlFactory.newConstant(skolemColumnValue, ISqlConstant.Type.UNKNOWN);
			String skolemValue = c.getValue().toString().substring(5, c.getValue().toString().length()-1);
			ISqlConstant c3 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
			ISqlConstant c4 = _sqlFactory.newConstant(skolemValue, ISqlConstant.Type.NUMBER);
	
			expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c1, c2));
			expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c3, c4));
		}else{
			if(comparable){
				if(fakeSkolemConstant){
					expr = _sqlFactory.newExpression(ISqlExpression.Code.IS_NULL, c1);
				}else{
					ISqlConstant c2 = _sqlFactory.newConstant(c.toString(), zType(attribute.getSQLTypeName()));
					expr = _sqlFactory.newExpression(ISqlExpression.Code.EQ, c1, c2);
				}
			}
	
			if(compareNullColumn){
				ISqlConstant c2 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
				ISqlConstant c3;
	
				if(fakeSkolemConstant){
					c3 = _sqlFactory.newConstant("-1", ISqlConstant.Type.NUMBER);
				}else{
					c3 = _sqlFactory.newConstant("1", ISqlConstant.Type.NUMBER);
				}
	
				expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c2, c3));
			}
		}
	
		return expr;
	}
	
	protected ISqlExp conditionsForVariable(RelationField attribute, AtomVariable var,
			int pos, boolean comparable, boolean compareNullColumn, Map<String,String> map)
	{
		ISqlExp expr = null;
		String l = fullNameForAttr(attribute, pos);
	
		if(var.isSkolem()){
			ISqlConstant c3 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
			//ISqlConstant c4 = _sqlFactory.newConstant(getSkolemTerm(var.skolemDef()), ISqlConstant.Type.COLUMNNAME);
			
			ISqlSimpleExpression c4 = _sqlFactory.newSimpleExpression(getSkolemTerm(var.skolemDef()));
	
	//		SqlExpression skolemExpr = new SqlExpression(SqlExpression.AND, new SqlExpression(SqlExpression.EQ, c1, c2), new SqlExpression(SqlExpression.EQ, c3, c4));
			ISqlExpression skolemExpr = _sqlFactory.newExpression(ISqlExpression.Code.EQ, c3, c4);
			expr = conjoin(expr, skolemExpr);			
		}else{
	//		otherwise, this is just a projected out var in a negated clause 
	//		-- why negated???
	
			if (!"-".equals(var.getName()) && !"_".equals(var.getName()) && map.containsKey(var.toString())){ 
				String n = map.get(var.toString());
	
				ISqlConstant c1 = _sqlFactory.newConstant(l, ISqlConstant.Type.COLUMNNAME);
				
				ISqlConstant.Type t = ISqlConstant.Type.COLUMNNAME;
				if (n.contains("LEAST("))
					t = ISqlConstant.Type.NUMBER;
				ISqlConstant c2 = _sqlFactory.newConstant(n, t);//ISqlConstant.Type.COLUMNNAME);
	
				if (c2 != null && c1.toString().compareTo(c2.toString()) == 0){
	//				this is the attribute/column in the map, 
	//				no need to add any condition
				}else{
					if(comparable)
						expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c1, c2));
	
					if(compareNullColumn){
						ISqlConstant c3 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
						ISqlConstant c4 = _sqlFactory.newConstant(n + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
	
	//					Do the following ever happen? 
	//					I suppose if we write NULL in the rule without quotes ...	
						if (l.equals("NULL"))
							c3 = _sqlFactory.newConstant("-1", ISqlConstant.Type.NUMBER);
						if (n.equals("NULL"))
							c4 = _sqlFactory.newConstant("-1", ISqlConstant.Type.NUMBER);
	
						expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c3, c4));
					}
				}
			}else if("-".equals(var.getName())){
				ISqlConstant c3 = _sqlFactory.newConstant(l + RelationField.LABELED_NULL_EXT, ISqlConstant.Type.COLUMNNAME);
				ISqlConstant c4 = _sqlFactory.newConstant("-1", ISqlConstant.Type.NUMBER);
				expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.EQ, c3, c4));
			}else{
	//			Var not in map or smth else, e.g. _
				assert(false);
			}
		}
		return expr;
	}
	
	public static String fullNameForAttr(RelationField attribute, int i){
		String l;
		if(i >= 0){
			l = "R" + i + "." + attribute.getName();
		}else{
			l = attribute.getName();
		}
		return l;
	}
	
	protected ISqlExp buildWhereForAtom(Atom a, int i, Map<String,String> map, ISqlExp expr, int curIterCnt){
		if(a.isSkolem()){ // this is a "fake" atom to represent a skolem term - ignore
			return expr;
		}else{
	//		boolean addIsNotNull = false;
	//		if(Config.getOuterJoin() && a.getRelation() instanceof ProvenanceRelation){
	//		ProvenanceRelation provRel = (ProvenanceRelation)a.getRelation();
	//		addIsNotNull = provRel.getType().equals(ProvRelType.OUTER_JOIN);
	//		}
	
			ISqlExp stratumExpr = stratumCondition(a, i, curIterCnt);
	
	//		No need to check - conjoin does that anyway
	//		if(stratumExpr != null)
			expr = conjoin(expr, stratumExpr);
	
	//		Need to add condition if one of the variables corresponds to a skolem term/atom
			for (int j = 0; j < a.getValues().size(); j++){
				RelationField attribute = a.getRelation().getField(j);
	
	//			if(addIsNotNull){
	//			SqlExpression cond = isNotNull(attribute,i);
	//			expr = conjoin(expr, cond);
	//			}
	
				boolean isSpecialAttrib;
	
	//			In the case of "Outer Union" we need to treat the MRULE attribute specially 
	//			(i.e., there is no labeled null column for that)	
	//			if(Config.getOuterUnion()){
				isSpecialAttrib = attribute.getName().equals(ProvenanceRelation.MRULECOLNAME);
	//			}else{
	//			isSpecialAttrib = false;
	//			}
	
	//			If this is an internal Orchestra rule, we only need to compare
	//			key attributes and labeled nulls, as an optimization
	//			For "external" rules issued by the user, compare everything
	
	//			"Normal" behaviour would be:
	//			boolean comparable = true;
	
				//boolean comparable = attribute.getName().equals("KID");
				boolean comparable;
	
				try{
					if(m_external){
	//					comparable = true;
						comparable = !m_rule.onlyKeyAndNulls() || a.getRelation().getPrimaryKey().getFields().contains(a.getRelation().getField(j));
					}else{
						comparable = a.getRelation().getPrimaryKey().getFields().contains(attribute);
					}
	
				}catch(NullPointerException e){
					throw e;
				}
	
				AtomArgument atomVal = a.getValues().get(j);
	
	//			Always compare null columns if the relation has null columns and the attribute
	//			is not a special OU attribute
				boolean compareNullCol = !isSpecialAttrib && a.getRelation().hasLabeledNulls() &&
					(!Config.useCompactNulls() || a.getRelation().isNullable(j));
	
				if (atomVal instanceof AtomConst) 
				{
					ISqlExp constConditions = conditionsForConstant(attribute, (AtomConst)atomVal, i, comparable, compareNullCol);
	
					expr = conjoin(expr, constConditions);
				} 
				else if (atomVal instanceof AtomVariable) 
				{
					ISqlExp varConditions = conditionsForVariable(attribute, (AtomVariable)atomVal, i, comparable && !isSpecialAttrib, compareNullCol, map);
	
					expr = conjoin(expr, varConditions);
	
				} else // Old skolems Skolem -- shouldn't matter for now
				{
					assert(false);
	//				l += RelationField.LABELED_NULL_EXT;
	//				String str = skolemExpr((ScMappingAtomSkolem)atomVal);
	//				SqlConstant c1 = new SqlConstant(l, SqlConstant.COLUMNNAME);
	//				SqlConstant c2 = new SqlConstant(str, SqlConstant.COLUMNNAME);
	
	//				if(isSpecialAttrib && a.getRelation().hasLabeledNulls()){
	//				if(comparable){
	//				expr = conjoin(expr, new SqlExpression(SqlExpression.EQ, c1, c2));
	//				}else{
	//				expr = conjoin(expr, hack(c1,c2));
	//				}
	//				}
				}
			}
	
		}
		return expr;
	}
	
	protected ISqlExp buildWhere(int curIterCnt) {
		ISqlExp expr = null;
	
		for (int i = 0; i < m_rule.getBody().size(); i++) 
		{
			Atom a = m_rule.getBody().get(i);
			try{
				if (a.isSkolem() || BuiltinFunctions.isBuiltIn(a.getSchema().getSchemaId(), a.getRelation().getName())) {
					;
				} else if(!a.isNeg()){
	//				SqlExpression oldExpr = OLDbuildWhereForAtom(a, i, m_varmap, expr, curIterCnt);
	//				expr = OLDbuildWhereForAtom(a, i, m_varmap, expr, curIterCnt);
					expr = buildWhereForAtom(a, i, m_varmap, expr, curIterCnt);
	
	//				if(expr != null && oldExpr != null && !expr.toString().equals(oldExpr.toString())){
	//				float x = 1/0;
	//				}
	
	//				} else if(i == m_rule.getBody().size()-1) { // Temporary HACK for Negated atom -- convert to except
	//				// Skip last atom - will add except at higher level
	
				}else { // Negated atom -- convert to SQL antisemijoin (not-exists)
					ISqlSelect n = _sqlFactory.newSelect();
	
					HashMap<String,String> atomMap = buildVarMapForAtom(a, i, null, true);
	//				n.addSelect(buildSelectForAtom(a, atomMap, false));
					n.addSelectClause(buildSelectForAtom(a, atomMap, true, false, curIterCnt));
					List<ISqlFromItem> v = newArrayList();
					v.add(buildFromItem(a, i));
					n.addFromClause(v);
	
	//				SqlExpression oldExpr = OLDbuildWhereForAtom(a, i, m_varmap, null, curIterCnt);
	//				SqlExpression newExpr = OLDbuildWhereForAtom(a, i, m_varmap, null, curIterCnt);
					ISqlExp newExpr = buildWhereForAtom(a, i, m_varmap, null, curIterCnt);
	
	//				if(newExpr != null && oldExpr != null && !newExpr.toString().equals(oldExpr.toString())){
	//				float x = 1/0;
	//				}
					n.addWhere(newExpr);
					if(Config.getNotExists()){
						expr = conjoin(expr, _sqlFactory.newExpression(ISqlExpression.Code.NOT, _sqlFactory.newExpression(ISqlExpression.Code.EXISTS, n)));
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		Iterator<ISqlExp> whereExps = _whereRoots.iterator();
	
		while (whereExps.hasNext()) {
			ISqlExp nx = whereExps.next();
	
			expr = conjoin(expr, nx);
		}
		return expr;
	}
	
	protected ISqlSelectItem stratumSelection(Atom a, boolean isHeadAtom, int curIterCnt)
	{
		boolean isStratifiedAtom = (a.getType() == AtomType.DEL || a.getType() == AtomType.INS);
	
		// Hack for stratum
		if(isStratifiedAtom  && Config.getStratified()){
			if(isHeadAtom){
				if(Config.getPrepare()){
					m_rule.addPreparedParam(0);
					return aliasedSelect(null, m_sqlString.preparedParameterProjection(), "", "STRATUM", false, true);
				}else{
					return aliasedSelect(null, Integer.toString(curIterCnt), "", "STRATUM", false, true);
				}
			}else{
	//			I don't think I need to project this		 
	//			return aliasedSelect(null, , "", "STRATUM", true, true);
			}
		}
		return null;
	}
	
	protected ISqlSelectItem selectionForVariable(Atom a, AtomVariable var, 
			RelationField attribute, Map<String, String> varmap, boolean fldInKey, boolean keysAndNulls){
		String col = varmap.get(var.toString());
	
	//	keysAndNulls = true is used for internal Orchestra rules
	
		if(var.isSkolem()){ // New Skolem case
			return skolemNull(attribute, false);
		}else{
	
	//		If this is an internal Orchestra maintenance rule, project only key attributes
	//		and put nulls in rest, otherwise project everything
	//		Note: some "internal" rules, for "delete from head", need to project everything, too
			if(fldInKey || !keysAndNulls || !m_rule.onlyKeyAndNulls()){
				return aliasedSelect(attribute, col, "", attribute.getName(), false, m_rule.replaceValsWithNullValues());
			}else{
	//			Includes case for:
	//			if (col == null || col.equals("NULL") || col.equals("null"))
	//			I don't think there is any reason to treat this separately
				return typedNull(attribute, false);
			}
		}
	}
	
	protected ISqlSelectItem selectionNullForVariable(AtomVariable var,
			RelationField attribute, Map<String, String> varmap){
		String col = varmap.get(var.toString());
		String lnExt = RelationField.LABELED_NULL_EXT;
		String lnAttName = attribute.getName() + RelationField.LABELED_NULL_EXT;
	
		if(var.isSkolem()){ // New Skolem case
			String skolemTerm = getSkolemTerm(var.skolemDef());
	
			ISqlSelectItem s = _sqlFactory.newSelectItem(skolemTerm);
			s.setAlias(lnAttName);
			return s;
		}else{
			return aliasedSelect(attribute, col, lnExt, 
					lnAttName, true, 
					m_rule.replaceValsWithNullValues());
		}
	}
	
	protected ISqlSelectItem selectionForConst(AtomConst c, 
			RelationField attribute, boolean fldInKey, boolean keysAndNulls)
	{
	
	
		if(keysAndNulls && m_rule.onlyKeyAndNulls() && !fldInKey)
		{
			return typedNull(attribute, false);
		}
		else if(m_rule.onlyKeyAndNulls()){
	
			if (c.getValue()!= null && !"-".equals(c.getValue()) && !"_".equals(c.getValue())){
				if(fldInKey)
					return aliasedSelect(attribute, c.toString(), "", attribute.getName(), false, m_rule.replaceValsWithNullValues());
				else
					return typedNull(attribute, false);
			}else{
				return aliasedSelect(attribute, c.toString(), "", attribute.getName(), false, false);
			}
		}else if (c.getValue()!=null && !"-".equals(c.getValue()) && !"_".equals(c.getValue())){
	//		Real constant case
			return aliasedSelect(attribute, c.toString(), "", attribute.getName(), false, m_rule.replaceValsWithNullValues());
		}else{
	//		This must be one part of the - for null case
			return typedNull(attribute, false);
		}
	
	
	}
	
	protected ISqlSelectItem selectionNullForConst(AtomConst c, RelationField attribute)
	{
		ISqlSelectItem s;
		if (c.isLabeledNull()){
			String skolemValue = c.getLabeledNullValue();
			s = _sqlFactory.newSelectItem(skolemValue);
		}else if (c.getValue()!=null && !"-".equals(c.getValue()) && !"_".equals(c.getValue())){
			s = _sqlFactory.newSelectItem("1");
		}else{
	//		And this is the second part of the - for labeled null ...
			s = _sqlFactory.newSelectItem("-1");
		}
		s.setAlias(attribute.getName() + RelationField.LABELED_NULL_EXT);
		return s;
	}
	
	
	protected List<ISqlSelectItem> buildSelectForAtom(Atom a, 
			Map<String, String> varmap, boolean keysAndNulls, 
			boolean isHeadAtom, int curIterCnt) {
		List<ISqlSelectItem> vs = newArrayList();
		List<ISqlSelectItem> vsNull = newArrayList();
	
		if(a.isSkolem()){ // this is a "fake" atom to represent a skolem term - ignore
			return null;
		}else if (a.isNeg()){ 
	//		This is a negated atom, that we translate into a 
	//		"not exists" query that doesn't need to return any data
	//		Maybe add that this is the "proper" translation only for 
	//		"internal" orchestra rules?	
			// && !m_rule.deleteFromHead()){
			vs.add(_sqlFactory.newSelectItem("1"));
		}else{
	
			for (int i = 0; i < a.getValues().size(); i++) {
				AtomArgument atomVal = a.getValues().get(i);
				RelationField attribute = a.getRelation().getField(i);
	
				boolean specialAttrib = false;
				boolean fldInKey;
	
				try{
	//				if(Config.getOuterUnion()){
					specialAttrib = attribute.getName().equals(ProvenanceRelation.MRULECOLNAME);
	//				}
	
					if (a.getRelation().getPrimaryKey() != null){
	
						fldInKey = a.getRelation().getPrimaryKey().getFields().contains(a.getRelation().getField(i));
	//					if(!fldInKey){
	//					for(RelationField f : a.getRelation().getSkolemizedFields()){
	//					if(f.getName().equals(attribute.getName())){
	//					fldInKey = true;
	//					break;
	//					}
	//					}
	//					}
					}else{
	//					If the primary key is null, assume that the whole thing is the key
						fldInKey = true;
					}
	
				}catch(NullPointerException e){
					e.printStackTrace();
					return null;
				}
	
				if (atomVal instanceof AtomVariable) 
				{
					vs.add(selectionForVariable(a, (AtomVariable)atomVal, attribute, varmap, fldInKey, keysAndNulls));
					if(!specialAttrib && (!Config.useCompactNulls() || a.isNullable(i)))
						vsNull.add(selectionNullForVariable((AtomVariable)atomVal, attribute, varmap));
	
				} 
				else if (atomVal instanceof AtomConst) 
				{
					boolean labNullConst = (((AtomConst)atomVal).isLabeledNull());
	
					if(labNullConst){
						vs.add(skolemNull(attribute, false));
						if(!specialAttrib)
							vsNull.add(selectionNullForConst((AtomConst)atomVal, attribute));
	
					}else{
						vs.add(selectionForConst((AtomConst)atomVal, attribute, fldInKey, keysAndNulls));
						if(!specialAttrib && (!Config.useCompactNulls() || a.isNullable(i)))
							vsNull.add(selectionNullForConst((AtomConst)atomVal, attribute));
					}
				} else { // Old Skolem case
					vs.add(typedNull(a.getRelation().getField(i), false));
					String str = skolemExpr((AtomSkolem)atomVal);
					if (!specialAttrib && (!Config.useCompactNulls() || a.isNullable(i)))
						vsNull.add(aliasedSelect(attribute, str, RelationField.LABELED_NULL_EXT, attribute.getName() + RelationField.LABELED_NULL_EXT, true, m_rule.replaceValsWithNullValues()));
				}
			}
			if (a.getRelation().hasLabeledNulls())
				vs.addAll(vsNull);
		}
		if (m_stratified) {
			ISqlSelectItem s = stratumSelection(a, isHeadAtom, curIterCnt);
			if(s != null)
				vs.add(s);
		}

		return vs;
	}
	
	protected List<ISqlSelectItem> buildSelect(int curIterCnt, List<ISqlFromItem> fr) {
		
//		if (m_rule.getHead().getRelation().getName().equals("pM1") && m_rule.getHead().getType().equals(AtomType.DEL))
//			System.out.println("Here");
		
		List<ISqlSelectItem> ret = buildSelectForAtom(m_rule.getHead(), m_varmap, true, true, curIterCnt);
	
	//	Add provenance attribute
		try{
		if (m_rule.getProvenance() != null) {
	
			if(Config.getValueProvenance()){
				String prov = "(" + m_rule.getProvenance().getSqlValueExpression(m_varmap, m_vartype, m_rule, fr) + ") PROV__";
				
				ret.add(_sqlFactory.newSelectItem(prov));
			}else{
				ret.add(_sqlFactory.newSelectItem("(" + m_rule.getProvenance().getSqlExpression(m_varmap, m_vartype,
				"CHAR") + ") PROV__"));
			}
	
		}
		
		return ret;
	//	return OLDbuildSelectForAtom(m_rule.getHead(), m_varmap, true, true, curIterCnt);
		}catch (InvalidAssignmentException e){
			e.getMessage();
			e.printStackTrace();
			return null;
		}
	}
	
	protected ISqlSelectItem typedNull(RelationField field, boolean useNLExt) {
		ISqlSelectItem s;
		if(useNLExt){
			s = _sqlFactory.newSelectItem("-1");
		}else{
			s = _sqlFactory.newSelectItem(m_sqlString.nullProjection(field.getSQLTypeName()));
		}
		s.setAlias(field.getName());
		return s;
	
	//	return aliasedSelect("cast(null as " 
	//	+ (useNLExt?"NUMERIC(10)":typesMap.get(field))         						
	//	+ ")", field.getName() + (useNLExt?RelationField.LABELED_NULL_EXT:""), true);
	}
	
	protected ISqlSelectItem skolemNull(RelationField field, boolean useNLExt) {
		ISqlSelectItem s;
		if(useNLExt){
			s = _sqlFactory.newSelectItem("-1");
		}else{
			s = _sqlFactory.newSelectItem(m_sqlString.skolemNullProjection(field.getSQLTypeName()));
		}
		s.setAlias(field.getName());
		return s;
	
	//	return aliasedSelect("cast(null as " 
	//	+ (useNLExt?"NUMERIC(10)":typesMap.get(field))         						
	//	+ ")", field.getName() + (useNLExt?RelationField.LABELED_NULL_EXT:""), true);
	}
	
	protected ISqlSelectItem aliasedSelect(RelationField field, String name, String ext, String alias, boolean isLabNull, boolean replaceValsWithNulls) {
		ISqlSelectItem s;
	
		/*
	        if (name == null)
	        	throw new RuntimeException("Unable to alias variable to " + alias + " because source is null");
	        if (alias == null)
	        	throw new RuntimeException("Unable to alias variable " + name + ": alias is null");
		 */
	
		if (name == null || name.equals("NULL") || name.equals("null")){        	
			s = typedNull(field, isLabNull);
		} else if(isLabNull){
			if(replaceValsWithNulls){
				s = _sqlFactory.newSelectItem(m_sqlString.caseNull(name));
				s.setAlias(alias);
			}else{
				s = _sqlFactory.newSelectItem(name + ext);
				s.setAlias(alias);        	
			}
		}else{
			s = _sqlFactory.newSelectItem(name + ext);
			s.setAlias(alias);        	
		}
		return s;
	}
	
	
	protected ISqlExp conjoin(ISqlExp expr, ISqlExp cond) {
		if (expr == null) {
			return cond;
		} else if (cond == null) {
			return expr; 
		} else {
			return _sqlFactory.newExpression(ISqlExpression.Code.AND, expr, cond);
		}
	}
	
	
	protected String skolemExpr(AtomSkolem sk) {
		// assume for now all parameters are plain variables
		StringBuffer buf = new StringBuffer();
	
		for (int i = 0 ; i <= sk.getParams().size() ; i++)
			buf.append ("CONCAT(");
	
		buf.append("'" + sk.getName() + "('");
		boolean f = true;
		for (AtomArgument val : sk.getParams()) 
		{
			buf.append (",");
			buf.append ((f?"":"CONCAT(',' ,"));
			if (val instanceof AtomVariable)
			{
				buf.append ("CONCAT(CONCAT(");
				buf.append ("'''', " + m_varmap.get(val.toString()));
				buf.append ( "), '''')");
			}
			else if (val instanceof AtomConst)
			{
				buf.append(val.toString());
			}
			else if (val instanceof AtomSkolem)
				buf.append (skolemExpr((AtomSkolem)val));
	
	
			buf.append ((f?"":")"));
			f = false;
			buf.append (")");
		}
		buf.append(",')')");
	
	
		return buf.toString();
	}
	
	public String getSkolemTerm(Atom skolemDef){
	//	throws SkolemException{
	
	//	Skip first attribute, which is the variable to which this skolem should be assigned
		Map<String, String> varmap = buildVarMap(m_rule);
	//	AbstractRelation rel = skolemDef.getRelation();
		StringBuffer buf = new StringBuffer();
	
		buf.append("SKOLEM.SKOLEM");
		int i = 0;
	//	Olivier: loop on skol values instead of rel fields
	
	//	for(int j = 0 ; j < skolemDef.getSkolemKeyVals().size(); j++){
		for(int j = 0 ; j < skolemDef.getValues().size()-1 ; j++){
	//		if(i > 0){
			buf.append("STR");
	//		if(f.getType() instanceof StringType){
	//		buf.append("STR");
	//		}else if(f.getType() instanceof DateType){
	//		buf.append("DAT");
	//		}else if(f.getType() instanceof IntType){
	//		buf.append("INT");
	//		}else{
	////		throw new SkolemException("Unsupported type in skolem term");
	//		return null;
	//		}
	//		}else{
	//		buf.append("STR");
	//		}
			i++;
		}
	
		i = 0;
		buf.append("(");
	
	//	Name too long?
	//	buf.append("'" + skolemDef.getRelation().getName() + "'");
		String shortName = skolemDef.getRelation().getName().substring(skolemDef.getRelation().getName().indexOf(':')+1);
		buf.append("'" + shortName + "'");
	
		List<AtomArgument> keyFields = skolemDef.getSkolemKeyVals();
	
		for(AtomArgument v : skolemDef.getValues()){
			if(i > 0){
				if(varmap.containsKey(v.toString())){
					if(keyFields.contains(v)){
	//					buf.append(", CASE WHEN " + varmap.get(v.toString()) + " IS NOT NULL THEN CHAR(" + varmap.get(v.toString()) + ") ELSE '' END ");
	
						//buf.append(", SUBSTR(CHAR(" + varmap.get(v.toString()) + "),1,CASE WHEN LENGTH(CHAR(" + varmap.get(v.toString()) + ")) > 128 THEN 128 ELSE LENGTH(CHAR(" + varmap.get(v.toString()) + ")) END )");
						//buf.append(", " + SqlQuery.getFirst128Chars("CHAR(" + varmap.get(v.toString()) + ")"));
						buf.append(", " + getFirst128Chars(varmap.get(v.toString())));
					}else{
	
	//					buf.append(", SUBSTR(CHAR(" + varmap.get(v.toString()) + RelationField.LABELED_NULL_EXT + "),1,CASE WHEN LENGTH(CHAR(" + varmap.get(v.toString()) + RelationField.LABELED_NULL_EXT + "))>128 THEN 128 ELSE LENGTH(CHAR(" + varmap.get(v.toString()) + RelationField.LABELED_NULL_EXT + ")) END )");
						buf.append(", " + "CHAR(" + varmap.get(v.toString()) + RelationField.LABELED_NULL_EXT + ")");
	
	
	//					buf.append(", CASE WHEN " + varmap.get(v.toString()) + " IS NOT NULL THEN CHAR(" + varmap.get(v.toString()) + ") ELSE '' END " +
	//					"|| CHAR(" + varmap.get(v.toString()) + RelationField.LABELED_NULL_EXT + ") ");
					}
				}else
					System.out.println("Variable for skolem param not found in varmap!");
			}
			i++;
		}
		buf.append(")");
	
		return buf.toString();
	
	}

	/**
	 * Cast the item into a (max 128-character) string, as appropriate
	 * 
	 * @param var
	 * @return
	 */
	public String getFirst128Chars(String var) {
		if (m_vartype.get(var) == null || m_vartype.get(var) instanceof ClobType) {
			return m_sqlString.getFirst128Chars("CHAR(" + var + ")");
		} else if (m_vartype.get(var) instanceof StringType) {
			return m_sqlString.getFirst128Chars(var);
		} else {
			return "CHAR(" + var + ")";
		}
	}
}
