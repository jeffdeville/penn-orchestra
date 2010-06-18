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
package edu.upenn.cis.orchestra.mappings;

import static edu.upenn.cis.orchestra.util.DomUtils.addChildWithText;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomSkolem;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.UntypedAtom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TypedRelation;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.dbms.IRuleCodeGen;
import edu.upenn.cis.orchestra.dbms.RuleSqlGen;
import edu.upenn.cis.orchestra.dbms.IRuleCodeGen.UPDATE_TYPE;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.PositionedString;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * 
 * @author gkarvoun
 *
 */
public class Rule extends Mapping
{

	private boolean _clearNcopy = false;
	//	private boolean _headEmpty = false;
	private boolean _deleteFromHead = false; 
	private boolean _onlyKeyAndNulls = false;
	private boolean _replaceValsWithNullValues = false;
	private boolean _edb = false;
	private boolean _distinct = true;
	private Mapping _parentMapping = null;

	private ProvenanceNode _provenanceTree = null;

	private IRuleCodeGen _code;
	private List<Integer> _preparedParams = new ArrayList<Integer>();

	private final Map<String, Schema> _builtInSchemas;
	public Rule (Atom head, Atom b, Mapping mapping, Map<String, Schema> builtInSchemas)
	{
		super(head.deepCopy(), b.deepCopy());
		_builtInSchemas = Collections.unmodifiableMap(builtInSchemas);
		_code = new RuleSqlGen(this, _builtInSchemas);
		setParentMapping(mapping);
	}

	public Rule (Atom head, List<Atom> body, Mapping mapping, Map<String, Schema> builtInSchemas)
	{
		_builtInSchemas = Collections.unmodifiableMap(builtInSchemas);
		_code = new RuleSqlGen(this, _builtInSchemas);
		_head.add(head.deepCopy());
		for (Atom atom : body)
			_body.add(new Atom(atom).deepCopy());

		setParentMapping(mapping);
	}

	public Rule (Atom head, List<Atom> body, Mapping mapping, boolean onlyKeyAndNulls, Map<String, Schema> builtInSchemas)
	{
		this (head, body, mapping, builtInSchemas);
		_code = new RuleSqlGen(this, builtInSchemas);
		_onlyKeyAndNulls = onlyKeyAndNulls;
		//_onlyKeyAndNulls = false;
	}

	//public IDb getDb() {
	//	return _db;
	//}

	public Map<String, Schema> getBuiltInSchemas() {
		return _builtInSchemas;
	}
	public ProvenanceNode getProvenance() {
		return _provenanceTree;
	}

	public void setProvenance(ProvenanceNode p) {
		_provenanceTree = p;
	}

	/**
	 * Get the rule's head 
	 * @return Rule's head
	 */
	public Atom getHead ()
	{
		return _head.get(0);
	}

	public void addPreparedParam(int i) {
		_preparedParams.add(i);
	}

	public List<Integer> preparedParams() {
		return Collections.unmodifiableList(_preparedParams);
	}

	public void setReplaceValsWithNullValues(){
		_replaceValsWithNullValues = true;
	}

	public boolean replaceValsWithNullValues(){
		return _replaceValsWithNullValues;
	}

	public void setEdb(){
		_edb = true;
	}

	public boolean isEdb(){
		return _edb;
	}

	public void setDistinct(boolean value){
		_distinct = value;
	}

	public boolean isDistinct(){
		return _distinct;
	}

	public void setOnlyKeyAndNulls(){
		_onlyKeyAndNulls = true;
	}

	public boolean onlyKeyAndNulls(){
		return _onlyKeyAndNulls;
	}

	public boolean getDeleteFromHead(){
		return _deleteFromHead;
	}

	public void setDeleteFromHead(){
		_deleteFromHead = true;
	}

	//	public boolean headEmpty(){
	//	return _headEmpty;
	//	}

	//	public void setHeadEmpty(){
	//	_headEmpty = true;
	//	}

	public boolean clearNcopy(){
		return _clearNcopy;
	}

	public void setClearNcopy(){
		_clearNcopy = true;
	}

	/**
	 * Return a new mapping with all variables renamed to a new, unique name
	 * @param asNewObject If true then a new freshen rule will be returned, if false this rule will be
	 * 					freshen and then returned 
	 * @return
	 */
	public Rule fresh (boolean asNewObject)
	{
		Map<String,String> newNames = new HashMap<String, String> ();
		return fresh (newNames, asNewObject);
	}


	/**
	 * Return a new mapping with all variables renamed to a new, unique name or to the name mapped 
	 * in varsNameMap if it exists in the map
	 * @param varsNameMap Map old variable name to new variable name. Note: will be completed with new names 
	 * 					  affected to variables which were not in the initial map
	 * @param asNewObject If true then a new freshen rule will be returned, if false this rule will be
	 * 					freshen and then returned 
	 * @return
	 */
	public Rule fresh (Map<String,String> varsNameMap, boolean asNewObject)
	{

		Atom newHead = getHead().fresh(varsNameMap, asNewObject);
		List<Atom> newBody = new ArrayList<Atom> ();
		for (Atom atom : getBody())
			newBody.add (atom.fresh(varsNameMap, asNewObject));

		List<RuleEqualityAtom> newEqAtoms = new ArrayList<RuleEqualityAtom> ();
		for (RuleEqualityAtom eq : getEqAtoms())
			newEqAtoms.add(eq.fresh (varsNameMap, asNewObject));

		if (asNewObject)
		{
			Rule res =  new Rule (newHead, newBody, getParentMapping(), onlyKeyAndNulls(), getBuiltInSchemas());		
			for (RuleEqualityAtom eq : newEqAtoms)
				res.addEqAtom(eq);		
			return res;
		}
		else
		{
			_head.remove(0);
			_head.add(newHead);
			_body = newBody;
			_eqAtoms = newEqAtoms;
			return this;
		}
	}


	/**
	 * Get the rule's body.<BR>
	 * Note: this is not a deep copy (even the list)
	 * @return Rule's body
	 */
	public List<Atom> getBody ()
	{
		return _body;
	}

	@Override
	public String toString ()
	{
		StringBuffer buffer = new StringBuffer ();
		buffer.append(getHead().toString());
		buffer.append (" :- ");

		boolean firstAtom = true;
		for (Atom atom : getBody())
		{
			buffer.append((firstAtom?"":", ") + atom.toString());
			firstAtom = false;
		}
		buffer.append("  ");
		firstAtom = true;
		for (RuleEqualityAtom eq : getEqAtoms())
		{
			buffer.append ((firstAtom?"":", ") + eq.toString());
			firstAtom = false;
		}

		if (_provenanceTree != null) {
			buffer.append(" | " + _provenanceTree.toString());
		}

		return buffer.toString();
	}

	public void printString ()
	{
		Debug.print(getHead().toString());
		Debug.print(" :- ");

		boolean firstAtom = true;
		for (Atom atom : getBody())
		{
			Debug.print((firstAtom?"":", ") + atom.toString());
			firstAtom = false;
		}
		Debug.print("  ");
		firstAtom = true;
		for (RuleEqualityAtom eq : getEqAtoms())
		{
			Debug.print ((firstAtom?"":", ") + eq.toString());
			firstAtom = false;
		}
		if (_provenanceTree != null) {
			Debug.print(" | " + _provenanceTree.toString());
		}
		Debug.print("\n");
	}


	/***
	 * Olivier: Added to integrate Greg's devt for SIGMOD 2007. Original method name was allVars.
	 * @return
	 */
	public List<AtomVariable> getAllBodyVariables(){
		List<AtomVariable> allVars = new ArrayList<AtomVariable>();
		Set<String> allVarNames = new HashSet<String> ();

		for (Atom atom : getBody())
			if(!atom.isSkolem()){
				for(AtomArgument val : atom.getValues())
				{
					//					ignore constants ... and there shouldn't be any skolems in the body anyway
					if (val instanceof AtomVariable && !allVarNames.contains(val.toString())) { 
						allVars.add((AtomVariable) val.deepCopy());
						allVarNames.add (val.toString());
					}
					else
						if (val instanceof AtomSkolem)
							assert false : "THERE SHOULD'NT BE ANY SKOLEM IN THE BODY FOR THIS FIRST IMPLEMENTATION";
				}
			}
		return allVars;
	}	

	public List<String> toUpdate(int curIterCnt) {
		UPDATE_TYPE u = (clearNcopy() ? UPDATE_TYPE.CLEAR_AND_COPY : 
			(getDeleteFromHead() ? UPDATE_TYPE.DELETE_FROM_HEAD : 
				UPDATE_TYPE.OTHER));
		_preparedParams.clear();
		List<String> ret = _code.getCode(u, curIterCnt);
		return ret;
	}

	public List<String> toUpdate(List<String> last, int curIterCnt, List<List<Integer>> preparedParams) {
		UPDATE_TYPE u = (clearNcopy() ? UPDATE_TYPE.CLEAR_AND_COPY : 
			(getDeleteFromHead() ? UPDATE_TYPE.DELETE_FROM_HEAD :
				UPDATE_TYPE.OTHER));
		_preparedParams.clear();
		List<String> ret =  _code.getCode(last, u, curIterCnt);

		//		preparedParams.addAll(_preparedParams);

		//		Hack - clearNcopy rules -> 3 sql queries. All other rules -> exactly 1 query
		preparedParams.add(_preparedParams);
		if(clearNcopy()){
			preparedParams.add(_preparedParams);
			preparedParams.add(_preparedParams);
			if(false /*Config.isDB2()*/){ // another two for "volatile card" + not logged initially
				preparedParams.add(_preparedParams);
				preparedParams.add(_preparedParams);
			}
		}
		_preparedParams = null;
		return ret;
	}

	public Rule deepCopy(){
		List<Atom> newbody = new ArrayList<Atom> ();
		for(Atom a : getBody())
			newbody.add(a.deepCopy());
		Rule res = new Rule(getHead().deepCopy(), newbody, getParentMapping(), onlyKeyAndNulls(), getBuiltInSchemas());
		res._clearNcopy = clearNcopy();
		res._deleteFromHead = getDeleteFromHead();
		//		res._headEmpty = headEmpty();
		res._onlyKeyAndNulls = onlyKeyAndNulls(); 
		res._parentMapping = getParentMapping();
		res._fakeMapping = _fakeMapping;
		if(getProvenance() != null)
			res.setProvenance(getProvenance().deepCopy());
		return res;
	}

	//	public Rule composeWith (int atomIndex, Rule rule)
	//	throws CompositionException
	//	{
	//	assert(getBody().get(atomIndex).getRelation() == rule.getHead().getRelation()) : 
	//	"Impossible to compose with different relations in the atoms!";
	//	// Freshen the mapping to compose with; to avoid variables confusions between the two mappings
	//	rule = rule.fresh(true);

	//	// Prepare the new body: atom at atomIndex replaced by freshen rule body
	//	List<Atom> newBody = new ArrayList<Atom> ();
	//	newBody.addAll(getBody());
	//	Atom removedAtom = newBody.get(atomIndex);
	//	newBody.remove(atomIndex);
	//	int indAtom=0;
	//	for (Atom atom : rule.getBody())
	//	newBody.add(atomIndex + indAtom++, atom);

	////	Create the new Rule
	////	This rule "depends" on more than one mapping ...
	////	Pick "this", but this may not be enough
	//	Rule res = new Rule (getHead(), newBody, getParentMapping(), onlyKeyAndNulls(), _db);

	//	// Copy the existing equalities atoms
	//	for (RuleEqualityAtom atom : getEqAtoms())
	//	res.addEqAtom(atom);

	//	// Add the equalities atoms to define equalities between the initial mapping variables
	//	// and the new composed mapping variables
	//	for (int i = 0 ; i < removedAtom.getValues().size() ; i++)
	//	res.addEqAtom(new RuleEqualityAtom(removedAtom.getValues().get(i), rule.getHead().getValues().get(i)));

	//	return res;
	//	}


	public void substituteVars(Map<String,AtomArgument> varmap){
		getHead().substituteVars(varmap);
		for(Atom a : getBody()){
			a.substituteVars(varmap);
		}
	}

	/*
	 *  Greg: Slightly better than composeWith, in that it actually substitutes
	 *  variables, while composeWith introduces equality atoms, and also works for
	 *  composition with atoms whose "definition" is a set of rules
	 */
	public List<Rule> substituteAtom(int pos, List<Rule> defs, boolean minimize) {// throws UnsupportedDisjunctionException{
		List<Rule> res = new ArrayList<Rule>();
		Rule common = this.deepCopy();

		//		Temporary treatment for unfolding in negated clauses, until we extend
		//		rules with disjunction
		//		Doesn't do the trick though - defs contains all rules, not just the ones with the "right" head ...
		//		if(common.getBody().get(pos).isNeg()){
		//		if(defs.size() > 1 || defs.get(0).getBody().size() > 1)
		//		throw new UnsupportedDisjunctionException("Unfolding UCQs in negated atoms requires disjunction");
		//		else{
		//		ScMappingAtom a = common.getBody().remove(pos);
		//		Rule def = defs.get(0);
		//		Map<String, ScMappingAtomArgument> varmap = def.getHead().varHomomorphism(a);

		////		null means no homomorphism, e.g. because relation is different
		//		if(varmap != null){
		//		Rule newDef = def.deepCopy();
		//		newDef.renameExistentialVars();
		//		newDef.substituteVars(varmap);
		//		newDef.getBody().get(0).negate();
		//		common.getBody().addAll(newDef.getBody());
		//		res.add(common);
		//		}
		//		}
		//		}else{
		Atom a = common.getBody().remove(pos);

		for(Rule def : defs){
			//			Map<String, ScMappingAtomValue> varmap = a.varHomomorphism(def.getHead());
			Map<String, AtomArgument> varmap = def.getHead().varHomomorphism(a);
			//			Map<String, AtomArgument> varmap = a.varHomomorphism(def.getHead());

			//			null means no homomorphism, e.g. because relation is different
			if(varmap != null){
				Rule newRule = common.deepCopy();
				Rule newDef = def.deepCopy();
				newDef.renameExistentialVars();
				newDef.substituteVars(varmap);
				newRule.getBody().addAll(pos, newDef.getBody());
				if(minimize)
					newRule.minimize();
				res.add(newRule);
			}else{
				//				if(Config.getEdbbits() 
				//				){
				List<RuleEqualityAtom> hom = def.getHead().varHomomorphismEq(a);
				if(hom != null){
					Rule newRule = common.deepCopy();
					Rule newDef = def.deepCopy();
					newDef.renameExistentialVars();
					newRule.getBody().addAll(pos, newDef.getBody());
					for(RuleEqualityAtom eq : hom){
						newRule.addEqAtom(eq);
					}
					newRule.eliminateEqualities();
					if(minimize)
						newRule.minimize();
					res.add(newRule);
					//					}
				}
			}
		}
		//		}
		return res;

	}

	public List<Rule> substituteSingleAtomDefs(List<Rule> defs, List<Rule> unusedDefs, boolean unfoldPosAtomsMultiRuleDefs) {
		List<Rule> res = new ArrayList<Rule>();
		Map<TypedRelation, List<Rule>> ruleMap = list2map(defs);

		Rule resInit = this.deepCopy();
		resInit.getBody().clear();
		res.add(resInit);

		for(Atom a : this.getBody()){
			//			ScMappingAtom a = common.getBody().remove(pos);
			TypedRelation key = new TypedRelation(a.getRelation(), a.getType());

			//			No unfolding is possible - just "copy" the old atom		
			boolean cont = ruleMap.containsKey(key);
			List<Rule> defL = ruleMap.get(key);

			//			Only unfold atoms with a single def with body.size() > 1
			//			if(!cont || ( // a.isNeg() && 
			//			Unfold positive atoms with multiple defs or body.size() > 1
			if(!cont || 
					((!unfoldPosAtomsMultiRuleDefs || a.isNeg()) &&
							(defL.size() > 1 || defL.get(0).getBody().size() > 1))
			){
				for(Rule newRule : res){
					newRule.getBody().add(a.deepCopy());
				}
			}else{
				boolean foundHom = false;
				List<Rule> oldRes = res;
				res = new ArrayList<Rule>();

				for(Rule def : defL){
					Map<String, AtomArgument> varmap = def.getHead().varHomomorphism(a);

					//					null means no homomorphism, e.g. because relation is different
					if(varmap != null){
						foundHom = true;
						List<Rule> newRes = new ArrayList<Rule>();

						//						Copy the set of result rules, and append the unfolding of def to them
						//						If there are multiple relevant defs, multiple copies of this set will be created
						for(Rule oldRule : oldRes){
							Rule newRule = oldRule.deepCopy();

							Rule newDef = def.deepCopy();
							newDef.renameExistentialVars();
							newDef.substituteVars(varmap);
							//							This obviously only works if the body has only one atom ... 					
							if(a.isNeg())
								newDef.getBody().get(0).negate();
							newRule.getBody().addAll(newDef.getBody());
							newRes.add(newRule);
						}
						res.addAll(newRes);
						//						oid comparison should be ok here
						//						HACK: we only want to get rid of "fake" mappings that we have introduced 
						//						for _L/_R - other mappings need to still be populated for provenance 
						//						viewing/querying
						String bodyRelName = def.getBody().get(0).getRelation().getName();
						if(bodyRelName.endsWith("_L") || bodyRelName.endsWith("_R"))
							unusedDefs.remove(def);
					}
				}

				//				No homomorphism was found for any def
				//				Just add the atom itself to all result rules
				if(!foundHom){
					res = oldRes;
					for(Rule newRule : res){
						newRule.getBody().add(a.deepCopy());
					}
				}
			}
		}
		return res;
	}

	public static Map<TypedRelation, List<Rule>> list2map(List<Rule> rules) {
		Map<TypedRelation, List<Rule>> ret = new HashMap<TypedRelation, List<Rule>>();

		for(Rule r : rules){
			TypedRelation key = new TypedRelation(r.getHead().getRelation(), r.getHead().getType());

			if(!ret.containsKey(key)){
				List<Rule> bucket = new ArrayList<Rule>();
				bucket.add(r);
				ret.put(key, bucket);
			}else{
				List<Rule> bucket = ret.get(key);
				bucket.add(r);
			}
		}
		return ret;
	}

	public static List<Rule> map2list(Map<TypedRelation, List<Rule>> map) {
		List<Rule> ret = new ArrayList<Rule>();

		for(TypedRelation tr : map.keySet()){
			ret.addAll(map.get(tr));
		}
		return ret;
	}

	//	public void renameExistentialVars(){
	//	for(ScMappingAtom a : getBody()){
	//	a.renameExistentialVars(this);
	//	}
	//	}

	public List<Rule> invertForReachabilityTest(boolean invertEdbs, boolean invertOthers, List<RelationContext> edbs) {
		//		We always seem to want onlyKeyAndNulls to be true, so call it here accordingly
		return Rule.invertMappingForReachabilityTest(this, invertEdbs, invertOthers, edbs, true, getBuiltInSchemas(), true);
	}

	public List<Rule> invertForReachabilityTest(boolean invertEdbs, boolean invertOthers, List<RelationContext> edbs, boolean joinWithNew) {
		//		We always seem to want onlyKeyAndNulls to be true, so call it here accordingly
		return Rule.invertMappingForReachabilityTest(this, invertEdbs, invertOthers, edbs, true, getBuiltInSchemas(), joinWithNew);
	}

	public static List<Rule> invertMappingForReachabilityTest(Mapping mapping, boolean invertEdbs, boolean invertOthers, 
			List<RelationContext> edbs, boolean onlyKeyAndNulls, Map<String, Schema> builtInSchemas, boolean joinWithNew) {
		List<Rule> res = new ArrayList<Rule> ();

		for(Atom a : mapping.getBody()){
			if(!a.isNeg() && !a.isSkolem()){
				if((invertEdbs && invertOthers) ||
						(edbs.contains(a.getRelationContext()) && invertEdbs) ||
						(!edbs.contains(a.getRelationContext()) && invertOthers)){
					Atom newHead = a.deepCopy();
					newHead.setType(AtomType.INV);

					List<Atom> newBody = mapping.copyMappingHead();
					for(Atom bodyAtom : newBody)
						bodyAtom.setType(AtomType.INV);


					/*
				boolean allFound = true;

				for(int i = 0; i < newHead.getValues().size() && allFound; i++){
					ScMappingAtomValue v = newHead.getValues().get(i);
					if (v instanceof ScMappingAtomValVariable){	
						ScMappingAtomValVariable var1 = (ScMappingAtomValVariable)v;
						boolean varFound = false;
						for(ScMappingAtomValue vv : bodyAtom.getValues()){
							if(vv instanceof ScMappingAtomValVariable){	
								ScMappingAtomValVariable var2 = (ScMappingAtomValVariable)vv;
								if(var1.getName().equals(var2.getName())){
									varFound = true;
									break;
								}
							}
						}
						allFound = allFound && varFound;
					}
				}

				if(!allFound){
					 */
					if(joinWithNew){
						Atom safeBody = newHead.deepCopy();
						safeBody.setType(AtomType.NEW);
						newBody.add(safeBody);
					}
					//}
					for(Atom na : newBody){
						na.deskolemizeAllVars();
					}

					Rule newRule = new Rule(newHead, newBody, mapping, onlyKeyAndNulls, builtInSchemas);

					for(AtomArgument v : newRule.getAllHeadVariables()){
						if(v instanceof AtomVariable){
							AtomVariable var = (AtomVariable)v;
							if(var.isSkolem()){
								newRule.getBody().add(var.skolemDef());
							}
						}
					}

					//					for(AtomArgument v : newRule.getAllBodyVariables()){
					//					if(v instanceof AtomVariable){
					//					AtomVariable var = (AtomVariable)v;
					//					if(var.isSkolem()){
					//					newRule.getBody().add(var.skolemDef());
					//					}
					//					}
					//					}



					res.add(newRule);
				}
			}else{
				//				int foo = 1;
			}
		}
		return res;

	}

	protected static void complain(PositionedString str, String expected) throws ParseException {
		throw new ParseException("Expected " + expected + " in string \'" + str.getString() 
				+ "\' at position " + str.getPosition(), str.getPosition());
	}

	/**
	 * Parse the rule string, which must only reference symbols in the catalog
	 * 
	 * @param catalog Orchestra system catalog
	 * @param rule Datalog rule corresponding to object
	 * @return
	 * @throws ParseException Unable to parse the datalog
	 * @throws RelationNotFoundException Couldn't find relation in catalog
	 */
	static public Rule parse(OrchestraSystem catalog, String rule) throws ParseException, RelationNotFoundException {
		return parse(catalog, rule, null);
	}

	/**
	 * Parse the rule string, which must reference symbols in the catalog or
	 * in the local view definitions
	 * 
	 * @param catalog Orchestra system catalog
	 * @param rule Datalog rule corresponding to object
	 * @param locals Local view definitions
	 * @return
	 * @throws ParseException Unable to parse the datalog
	 * @throws RelationNotFoundException Couldn't find relation in catalog or views
	 */
	static public Rule parse(OrchestraSystem catalog, String rule, 
			Map<String,RelationContext> locals) throws ParseException, RelationNotFoundException {
		Holder<Integer> counter = new Holder<Integer>(0);
		PositionedString str = new PositionedString(rule);
		str.skipWhitespace();
		UntypedAtom h = UntypedAtom.parse(str, counter);
		boolean negateHead = false;
		if (h.getName().startsWith("NOT_")) {
			h.setName(h.getName().substring(4));
			negateHead = true;
		}

		str.skipWhitespace();
		if (!str.skipString(":-")) {
			complain(str, "':-'");
		}
		str.skipWhitespace();
		ArrayList<Atom> body = new ArrayList<Atom>();
		boolean first = true;
		str.skipWhitespace();
		while (str.inRange()) {
			if (first) {
				first = false;
			} else {
				if (!str.skipString(",")) {
					complain(str, "','");
				}
				str.skipWhitespace();
			}
			UntypedAtom b = UntypedAtom.parse(str, counter);
			boolean isNegated = false;
			if (b.getName().startsWith("NOT_")) {
				isNegated = true;
				b.setName(b.getName().substring(4));
			}
			Atom n = b.getTyped(catalog, locals);
			n.setNeg(isNegated);
			body.add(n);
			str.skipWhitespace();
		}
		Atom th;
		try {
			th = h.getTyped(catalog, locals);
		} catch (ParseException e) {
			th = h.getTyped(body);
			// Add this as a local view definition
			if (!locals.containsKey(th.getRelationContext().toString()))
				locals.put(th.getRelationContext().toString(), th.getRelationContext());
		} catch (RelationNotFoundException e) {
			th = h.getTyped(body);
			// Add this as a local view definition
			if (!locals.containsKey(th.getRelationContext().toString()))
				locals.put(th.getRelationContext().toString(), th.getRelationContext());
		}
		th.setNeg(negateHead);

		return new Rule(th, body, null, catalog.getMappingDb().getBuiltInSchemas());
	}

	public boolean equivalent(Object oth){
		Rule other;
		if(oth instanceof Rule)
			other = (Rule)oth;
		else
			return false;

		if(!getHead().equals(other.getHead()))
			return false;

		List<Atom> thisBody = getBody();
		List<Atom> otherBody = other.getBody();

		//		Is there a homomorphism each way?
		if(!isContainedAtom(this.getHead(), other.getHead(), other) ||
				!isContainedAtom(other.getHead(), this.getHead(), this))
			return false;

		boolean containedThis = true;
		for(Atom a: thisBody){
			boolean containedAtom = false;
			for(Atom b: otherBody){
				if(isContainedAtom(a, b, other)){
					containedAtom = true;
					break;
				}
			}
			if(!containedAtom){
				containedThis = false;
				break;
			}
		}

		boolean containedOther = true;
		if(containedThis){
			for(Atom a: otherBody){
				boolean containedAtom = false;
				for(Atom b: thisBody){
					if(isContainedAtom(a, b, this)){
						containedAtom = true;
						break;
					}
				}
				if(!containedAtom){
					containedOther = false;
					break;
				}
			}
		}
		if(containedOther && containedThis){
			Debug.println("FOUND EQUIVALENT RULES:");
			Debug.println(this.toString());
			Debug.println(other.toString());
			return true; 
		}else{
			return false;
		}
		//
		//		if(thisBody.size() != otherBody.size())
		//			return false;
		//
		//		for(int i = 0; i < thisBody.size(); i++){
		//			if(!thisBody.get(i).equals(otherBody.get(i))){
		//				return false;
		//			}
		//		}
		//		return true;
	}

	public boolean equals(Object oth){
		Rule other;
		if(oth instanceof Rule)
			other = (Rule)oth;
		else
			return false;

		if(!getHead().equals(other.getHead()))
			return false;

		List<Atom> thisBody = getBody();
		List<Atom> otherBody = other.getBody();

		if(thisBody.size() != otherBody.size())
			return false;

		for(int i = 0; i < thisBody.size(); i++){
			if(!thisBody.get(i).equals(otherBody.get(i))){
				return equivalent(oth);
			}
		}
		return true;
	}

	public Mapping getParentMapping(){
		return _parentMapping;
	}

	public void setParentMapping(Mapping mapping){
		_parentMapping = mapping;
		if(mapping != null && mapping.isFakeMapping())
			setFakeMapping(true);
	}

	public boolean isContainedAtom(Atom contained, Atom containing, Rule containingRule){
		if(!contained.getRelation().equals(containing.getRelation())||
				!contained.getType().equals(containing.getType()))
			return false;
		if(contained.getSchema()!=null && containing.getSchema()!=null && !contained.getSchema().getSchemaId().equals(containing.getSchema().getSchemaId()))
			return false;
		for(int i = 0; i < contained.getValues().size(); i++){
			AtomArgument arg1 = contained.getValues().get(i);
			AtomArgument arg2 = containing.getValues().get(i);
			if(arg1 instanceof AtomVariable){
				AtomVariable var1 = (AtomVariable)arg1;
				if(arg2 instanceof AtomConst){
					//					containing has const => filtering condition => more restricted
					return false;
				}else if(arg2 instanceof AtomVariable){
					AtomVariable var2 = (AtomVariable)arg2;
//					if((containingRule.isDistinguished(var2) || containingRule.isJoinVar(var2)) && !var2.equals(var1))
					if(var2.isExistential() && !var2.equals(var1))
						return false;
				}
			}else if(arg1 instanceof AtomConst){
				if(arg2 instanceof AtomConst){
					AtomConst c1 = (AtomConst)arg1;
					AtomConst c2 = (AtomConst)arg2;
					if(!c1.equals(c2)){
						//						if(contained.getRelation() instanceof ProvenanceRelation){
						//							ProvenanceRelation provRel = (ProvenanceRelation)contained.getRelation();
						//							if(ProvRelType.OUTER_JOIN.equals(provRel.getType())){
						//								if(c1.equals("0"))
						//									return false;
						//							}else{
						//								Debug.println("check");
						//							}
						//						}else{
						return false;
						//						}
					}
				}
			}
		}
		return true;
	}

	public boolean isIdenticalAtom(Atom contained, Atom containing){
		if(!contained.getRelation().equals(containing.getRelation())||
				!contained.getType().equals(containing.getType()))
			return false;
		if(contained.getSchema()!=null && containing.getSchema()!=null && !contained.getSchema().getSchemaId().equals(containing.getSchema().getSchemaId()))
			return false;
		for(int i = 0; i < contained.getValues().size(); i++){
			AtomArgument arg1 = contained.getValues().get(i);
			AtomArgument arg2 = containing.getValues().get(i);
			if(arg1 instanceof AtomVariable){
				AtomVariable var1 = (AtomVariable)arg1;
				if(arg2 instanceof AtomConst){
					//					containing has const => filtering condition => more restricted
					return false;
				}else if(arg2 instanceof AtomVariable){
					AtomVariable var2 = (AtomVariable)arg2;
					if(isDistinguished(var2) && !var2.equals(var1))
						return false;
				}
			}else if(arg1 instanceof AtomConst){
				if(arg2 instanceof AtomConst){
					AtomConst c1 = (AtomConst)arg1;
					AtomConst c2 = (AtomConst)arg2;
					if(!c1.equals(c2)){
						if(contained.getRelation() instanceof ProvenanceRelation){
							ProvenanceRelation provRel = (ProvenanceRelation)contained.getRelation();
							if(ProvenanceRelation.isJoinRel(provRel.getType())) {
								if(c1.equals("0"))
									return false;
							}else{
								Debug.println("check");
							}
						}else{
							return false;
						}
					}
				}
			}
		}
		return true;
	}

	public Atom mergeProvRelAtoms(Atom atom1, Atom atom2){
		List<AtomArgument> mergedArgs = new ArrayList<AtomArgument>();
		//		ProvenanceRelation provRel1;
		//		ProvenanceRelation provRel2;
		if(!atom1.getRelation().equals(atom2.getRelation()) ||
				!atom1.getType().equals(atom2.getType()) ||
				!(atom1.getRelation() instanceof ProvenanceRelation) ||
				!(atom2.getRelation() instanceof ProvenanceRelation)
		){
			return null;	
			//			}else if(!(atom1.getRelation() instanceof ProvenanceRelation) || 
			//			!(atom2.getRelation() instanceof ProvenanceRelation)){
			//			return null;
			//			}else{
			//			provRel1 = (ProvenanceRelation)atom1.getRelation();
			//			provRel2 = (ProvenanceRelation)atom2.getRelation();
		}
		for(int i = 0; i < atom1.getValues().size(); i++){
			AtomArgument arg1 = atom1.getValues().get(i);
			AtomArgument arg2 = atom2.getValues().get(i);
			if(arg1 instanceof AtomVariable){
				AtomVariable var1 = (AtomVariable)arg1;
				if(arg2 instanceof AtomVariable){
					AtomVariable var2 = (AtomVariable)arg2;
					if(!var2.equals(var1)){
						if(var1.isExistential()){
							mergedArgs.add(var2);
						}else if(var2.isExistential()){
							mergedArgs.add(var1);
						}else{
							return null;
						}
					}else{
						mergedArgs.add(var2);
					}
				}else{
					return null;
				}
			}else if(arg1 instanceof AtomConst){
				AtomConst c1 = (AtomConst)arg1;
				if(arg2 instanceof AtomConst){				
					AtomConst c2 = (AtomConst)arg2;
					if(!c1.equals(c2)){
						return null;
					}else{ // equal constants
						mergedArgs.add(arg1);
					}
				}else{ 
					return null;
				}
			}
		}
		return new Atom(atom1.getRelationContext(), mergedArgs, atom1.getType());
	}

	public Atom mergeAtoms(Atom atom1, Atom atom2){
		List<AtomArgument> mergedArgs = new ArrayList<AtomArgument>();
		//		ProvenanceRelation provRel1;
		//		ProvenanceRelation provRel2;
		if(!atom1.getRelation().equals(atom2.getRelation()) ||
				!atom1.getType().equals(atom2.getType()) ){
			return null;	
			//			}else if(!(atom1.getRelation() instanceof ProvenanceRelation) || 
			//			!(atom2.getRelation() instanceof ProvenanceRelation)){
			//			return null;
			//			}else{
			//			provRel1 = (ProvenanceRelation)atom1.getRelation();
			//			provRel2 = (ProvenanceRelation)atom2.getRelation();
		}
		for(int i = 0; i < atom1.getValues().size(); i++){
			AtomArgument arg1 = atom1.getValues().get(i);
			AtomArgument arg2 = atom2.getValues().get(i);
			if(arg1 instanceof AtomVariable){
				AtomVariable var1 = (AtomVariable)arg1;
				if(arg2 instanceof AtomConst){
					if(!isDistinguished(var1))
						mergedArgs.add(arg2);
					else
						return null;
				}else if(arg2 instanceof AtomVariable){
					AtomVariable var2 = (AtomVariable)arg2;
					if(isDistinguished(var1) && isDistinguished(var2) && !var2.equals(var1)){
						return null;
					}else if(isDistinguished(var1)) { // either var2 is existential or both vars are equal
						mergedArgs.add(var1);
					}else{
						mergedArgs.add(var2);
					}
				}
			}else if(arg1 instanceof AtomConst){
				if(arg2 instanceof AtomConst){
					AtomConst c1 = (AtomConst)arg1;
					AtomConst c2 = (AtomConst)arg2;
					if(!c1.equals(c2)){ // The following unfolding only makes sense for provenance relations
						if(atom1.getRelation() instanceof ProvenanceRelation){
							ProvenanceRelation provRel1 = (ProvenanceRelation)atom1.getRelation();
							if(ProvenanceRelation.isJoinRel(provRel1.getType())){
								if((c1.equals("0") && c2.equals("1")) || (c1.equals("1") && c2.equals("0"))){
									mergedArgs.add(new AtomConst("1"));
								}
							}
						}else{
							return null;
						}
					}else{ // equal constants
						mergedArgs.add(arg1);
					}
				}else if(arg2 instanceof AtomVariable){
					AtomVariable var2 = (AtomVariable)arg2;
					if(!isDistinguished(var2))
						mergedArgs.add(arg1);
					else
						return null;
				}
			}
		}
		return new Atom(atom1.getRelationContext(), mergedArgs, atom1.getType());
	}

	public void minimize(){
		Debug.println("Rule before minimization: " + this);
		Debug.println("#atoms before minimization: " + getBody().size());
		
		List<Atom> newBody = new ArrayList<Atom>();
		for(int i = 0; i < getBody().size(); i++){
			boolean discard = false;
			Atom a = getBody().get(i);
			for(int j = i+1; j < getBody().size() && !discard; j++){
				Atom b = getBody().get(j);
				if(a.equals(b)){
					discard = true;
				}
			}
			if(!discard)
				newBody.add(a);

		}
		this.setBody(newBody);
		Debug.println("Rule after minimization: " + this);
		Debug.println("#atoms after minimization: " + getBody().size());
	}
	
	public List<Atom> minimize(List<Atom> discarded){
		Debug.println("Rule before minimization: " + this);
		Debug.println("#atoms before minimization: " + getBody().size());
		
		List<Atom> newBody = new ArrayList<Atom>();
		for(int i = 0; i < getBody().size(); i++){
			boolean discard = false;
			Atom a = getBody().get(i);
			for(int j = i+1; j < getBody().size() && !discard; j++){
				Atom b = getBody().get(j);
				if(a.equals(b)){
					discard = true;
				}
			}
			if(!discard)
				newBody.add(a);

		}
		this.setBody(newBody);
		List<Atom> ret = mergeComplementaryProvRelAtoms(discarded);
		Debug.println("Rule after minimization: " + this);
		Debug.println("#atoms after minimization: " + getBody().size());
		return ret;
	}

	/*
	 * Unfolding on mappings produces multiple atoms of the same provenance relation,
	 * when there are more than one atoms in the head. This method identifies such copies 
	 * and merges them into one atom
	 */
	public List<Atom> mergeComplementaryProvRelAtoms(List<Atom> discarded){
		List<Atom> body = getBody();
		//		boolean someMerged = false;
		List<Atom> ret = new ArrayList<Atom>();

		for(int i = 0; i < body.size(); ){ 
			Atom a = body.get(i);
			boolean merged = false;
			for(int j = i+1; j < body.size(); ){
				Atom b = body.get(j);
				Atom mergedAtom = mergeProvRelAtoms(a, b);
				if(mergedAtom != null){
					//					body.add(mergedAtom);
					discarded.add(body.get(i));
					discarded.add(body.get(j));
					body.set(i, mergedAtom);
					body.remove(j);
					ret.add(mergedAtom);
					//					body.remove(i);
					merged = true;
					
					break;
				}else{
					j++;
				}
			}
			if(!merged){
				i++;
			}
			//			someMerged = someMerged || merged;
		}
		return ret;
	}

	public void oldMinimize(){
		List<Atom> newBody = new ArrayList<Atom>();
		for(Atom a : getBody()){
			boolean discard = false;
			for(Atom b : getBody()){
				if(!a.equals(b) && !discard){
					if(isContainedAtom(b, a, this)){
						discard = true;
					}
				}
			}
			if(!discard)
				newBody.add(a);
		}
		this.setBody(newBody);
	}

	public void aggressiveMinimize(){
		List<Atom> body = getBody();
		//		boolean someMerged = false;

		for(int i = 0; i < body.size(); ){ 
			Atom a = body.get(i);
			boolean merged = false;
			for(int j = i+1; j < body.size(); ){
				Atom b = body.get(j);
				Atom mergedAtom = mergeAtoms(a, b);
				if(mergedAtom != null){
					//					body.add(mergedAtom);
					body.set(i, mergedAtom);
					body.remove(j);
					//					body.remove(i);
					merged = true;
					break;
				}else{
					j++;
				}
			}
			if(!merged){
				i++;
			}
			//			someMerged = someMerged || merged;
		}
	}

	public void oldMinimize2(){
		List<Atom> oldBody = getBody();
		//		List<Atom> newBody = new ArrayList<Atom>();
		boolean someMerged = false;

		for(int i = 0; i < oldBody.size(); ){ //i++){
			Atom a = oldBody.get(i);
			boolean merged = false;
			boolean someMergedInLastIter;
			do{
				someMergedInLastIter = false;
				for(int j = i+1; j < oldBody.size();){
					Atom b = oldBody.get(j);
					Atom mergedAtom = mergeAtoms(a, b);
					if(mergedAtom != null){
						//						newBody.add(mergedAtom);
						oldBody.add(mergedAtom);
						oldBody.remove(j);
						merged = true;
						someMergedInLastIter = true;
						break;
					}else{
						j++;
					}
				}
				if(!merged){
					//					newBody.add(a);
					i++;
				}else{
					oldBody.remove(i);
				}
				someMerged = someMerged || merged;
			}while(someMergedInLastIter);
		}
		//		if(someMerged)
		//		this.setBody(newBody);
	}

	/**
	 * Get the names of all database tables in the body
	 * 
	 * @return List of table names (prefixed by schema)
	 */
	public List<String> getBodyTables() {
		List<String> tableNames = new ArrayList<String>();
		for (Atom a: getBody()) {
			if(!a.isNeg() && !a.isSkolem())
				tableNames.add(a.toString3());
		}
		return tableNames;
	}

	/**
	 * Get the names of all database tables in the body
	 * 
	 * @return List of table names (prefixed by schema)
	 */
	public List<String> getHeadTables() {
		List<String> tableNames = new ArrayList<String>();
		if(clearNcopy()){
			tableNames.add(getBody().get(0).toString3());
		}
		tableNames.add(getHead().toString3());
		return tableNames;
	}

	@Override
	public Element serializeVerbose(Document doc) {
		Element rule = super.serializeVerbose(doc);
		rule.setAttribute("type", "rule");

		rule.setAttribute("clearNCopy", Boolean.toString(_clearNcopy));
		rule.setAttribute("deleteFromHead", Boolean.toString(_deleteFromHead));
		rule.setAttribute("onlyKeyAndNulls", Boolean.toString(_onlyKeyAndNulls));
		rule.setAttribute("replaceValsWithNullValues", Boolean.toString(_replaceValsWithNullValues));
		rule.setAttribute("edb", Boolean.toString(_edb));
		rule.setAttribute("distinct", Boolean.toString(_distinct));
		
		if (_provenanceTree != null) {
			rule.setAttribute("provenanceTree", _provenanceTree.toString());
		}
		if (!_preparedParams.isEmpty()) {
			rule.setAttribute("preparedParams", _preparedParams.toString());
		}
		
		if (_parentMapping != null) {
			Element parentMapping = (Element)rule.appendChild(_parentMapping.serializeVerbose(doc));
			parentMapping.setAttribute("relationship", "parentMapping");
		}
		return rule;
	}

	/**
	 * Returns an {@code Element} representing the code this {@code Rule} generates.
	 * 
	 * @param doc only used for {@code Element} creation.
	 * @return an {@code Element} representing the code this {@code Rule} generates
	 */
	public Element serializeAsCode(Document doc) {
		Element sql = doc.createElement("code");
		//Element ruleElement = serializeVerbose(doc);
		//sql.appendChild(ruleElement);
		List<String> statements = toUpdate(1);
		for (String statement : statements) {
			addChildWithText(doc, sql, "statement", statement);
		}
		Element ruleElement = serializeVerbose(doc);
		sql.appendChild(ruleElement);
		return sql;
	}
	
	/**
	 * Returns the {@code Rule} represented by {@code rule}.
	 * 
	 * @param rule
	 * @param system
	 * @return the {@code Rule} represented by {@code rule}
	 * @throws XMLParseException
	 */
	public static Rule deserializeVerboseRule(Element rule, OrchestraSystem system) throws XMLParseException {
		try {
			boolean cleanNCopy = DomUtils.getBooleanAttribute(rule,
					"clearNCopy");

			boolean deleteFromHead = DomUtils.getBooleanAttribute(rule,
					"deleteFromHead");
			boolean onlyKeyAndNulls = DomUtils.getBooleanAttribute(rule,
					"onlyKeyAndNulls");
			boolean replaceValsWithNullValues = DomUtils.getBooleanAttribute(
					rule, "replaceValsWithNullValues");
			boolean edb = DomUtils.getBooleanAttribute(rule, "edb");
			boolean distinct = DomUtils.getBooleanAttribute(rule, "distinct");
			Element parentMappingElement = DomUtils.getChildElementByName(rule,
					"mapping");
			//This mapping is really only a holder for (some of) this Rule's attributes.
			//A better way to handle this?
			Mapping mapping = Mapping.deserializeVerboseMappingImpl(rule, system);
			Mapping parentMapping = null;
			if (parentMappingElement != null) {
				parentMapping = Mapping.deserializeVerboseMapping(parentMappingElement, system);
			}

			Map<String, Schema> builtInSchemas = system.getMappingDb().getBuiltInSchemas();

			Rule result = new Rule(mapping.getMappingHead().get(0), mapping
					.getBody(), parentMapping, onlyKeyAndNulls, builtInSchemas);
			result.setId(mapping.getId());
			inializeFromElement(rule, result, system);
			if (cleanNCopy) {
				result.setClearNcopy();
			}
			if (deleteFromHead) {
				result.setDeleteFromHead();
			}
			if (replaceValsWithNullValues) {
				result.setReplaceValsWithNullValues();
			}
			if (edb) {
				result.setEdb();
			}
			result.setDistinct(distinct);
			return result;
		} catch (XMLParseException e) {
			throw e;
		} catch (Exception e) {
			throw new XMLParseException(e);
		}
	}
}

