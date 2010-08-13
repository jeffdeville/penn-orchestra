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
package edu.upenn.cis.orchestra.datalog.atom;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.mappings.RuleEqualityAtom;
import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.PositionedString;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Mapping definition : Mapping atom
 * @author Olivier Biton
 *****************************************************************
 */
public class Atom {


	/**
	 * Relation referenced by this atom
	 */
	private RelationContext _relationContext;
	/**
	 * List of values (variables, skolems...) 
	 */
	private List<AtomArgument> _values = new Vector<AtomArgument> ();	

	/**
	 * Determination of whether this value can be a labeled null
	 */
	private List<Boolean> _isNullable = new Vector<Boolean> ();
	
	private List<AtomAnnotation> _annotations = new ArrayList<AtomAnnotation>();
	
	/**
	 * Atom type used for delta rules. When a rule is not a delta rule, type will be AtomType.NONE
	 */
	
	/**
	 * Is this atom negated
	 */
	private boolean _neg=false;
	
	/**
	 * Is this atom one that has had annotations applied
	 */
	private boolean _isAnnotated=false;
	
	/**
	 * This atom is part of a bidirectional mapping and should be deleted as part 
	 * of "backward" propagation of deletions
	 */
	private boolean _del=false;
	
	private List<AtomArgument> _skolemKeyValues = null;
	
	private boolean _allStrata;
	
	public enum AtomType 
				{
					NONE,  // ALL
//					OLD,
					NEW,   // 
					INS,   // ALL
					DEL,   // ALL
					RCH,   // Only IDBs
					INV,   // ALL
					ALLDEL, // Not used in STRATIFIED deletions
//					BCK, // Backup
					D // Results of update policy
				};
				
	/**
	 * Atom type used for delta rules. When a rule is not a delta rule, type will be AtomType.NONE
	 */			
	protected AtomType _type;

	
	//TODO: Check if peer/schema/relation coherent
	//TODO: Check if nb values = nb fields in relation
	
	/**
	 * Creates a new Mapping atom
	 * @param peer Peer containing the schema/relation referenced by this atom
	 * @param schema Schema containing the relation referenced by this atom
	 * @param relation Relation referenced by this atom
	 * @param values List of values (variables, skolems...) 
	 */
	public Atom (Peer peer, Schema schema, Relation relation, List<AtomArgument> values)
	{
		this (new RelationContext (relation, schema, peer, false), values);
	}
	
	/**
	 * Creates a new Mapping atom
	 * @param relationContext Relation referenced by this atom with its full context
	 * @param values List of values (variables, skolems...) 
	 */
	public Atom (RelationContext relationContext, List<AtomArgument> values, AtomType type)
	{
		_relationContext = relationContext;
		_values.addAll(values);
		for (int i = 0; i < values.size(); i++)
			_isNullable.add(new Boolean(false));
		_type = type;
		_allStrata = true;
		_neg = false;
		_skolemKeyValues = null;
		setValuesTypes();
	}	
	
	private void setValuesTypes ()
	{
		int realSize;
		realSize = getRelation().getFields().size();
		if(Config.getEdbbits())
			realSize -= 1;
		
		if (getValues().size() < realSize) {
			realSize = getValues().size();
		}
		
		for (int i = 0 ; i < realSize ; i++)
		{
			getValues().get(i).setType(getRelation().getField(i).getType());
		}
	}
	
	public Atom (RelationContext relationContext, List<AtomArgument> values)
	{
		this(relationContext, values, AtomType.NONE);
	}	

	
	/**
	 * Creates a deep copy of a given atom. <BR>
	 * To benefit from polymorphism, use method <code>deepCopy (OrchestraSystem)</code>
	 * @param atom Atom to deep copy
	 * @param system Deep copy of the system containing the original mapping. 
	 * 				 Used to get find the peer/schema/relation referenced by the new mapping atom.
	 * @see Atom#deepCopy(OrchestraSystem) 
	 * @see Atom#deepCopy() 
	 */
	protected Atom (Atom atom, OrchestraSystem system)
	{
		Peer peer = system.getPeer(atom.getPeer().getId());
		Schema schema = peer.getSchema(atom.getSchema().getSchemaId());
		Relation relation = null;
		try{
			relation = schema.getRelation(atom.getRelation().getName());
		}catch(RelationNotFoundException e){
			e.printStackTrace();
		}
		_relationContext = new RelationContext (relation, schema, peer, false);
		if(atom.getSkolemKeyVals() == null)
			_skolemKeyValues = null;
		else
			_skolemKeyValues = new ArrayList<AtomArgument>();		
		
		int i = 0;
		for (AtomArgument val : atom.getValues()){
			AtomArgument vc = val.deepCopy();
			_values.add (vc);
			_isNullable.add(atom._isNullable.get(i++));
			
			if(atom.getSkolemKeyVals() != null	&& atom.getSkolemKeyVals().contains(val))
				_skolemKeyValues.add (vc);
		}
		
		_type = AtomType.NONE;
		_neg = atom.isNeg();
		_del = atom.getDel();
		_allStrata = atom.allStrata();
		_annotations.addAll(atom.getAnnotations());
		setValuesTypes();
		
	}
	
	/**
	 * Creates a deep copy of a givem atom but keeping a reference to the same relation. <BR>
	 * If the full system is being copied, call deepCopy (OrchestraSystem) to
	 * update the relations references.<BR>
	 * To benefit from polymorphism, use method <code>deepCopy ()</code>
	 * @param atom Atom to deep copy
	 * @see Atom#deepCopy(OrchestraSystem)
	 * @see Atom#deepCopy()
	 */
	public Atom (Atom atom)
	{
//		this(atom, atom.getValues(), AtomType.NONE);
		this(atom, atom.getValues(), atom.getType());
	}

	//TODO Exception if peer ref or rel unknown

	
	/**
	 * Creates a new mapping Atom as a copy of <code>atom</code>, with a new set of values 
	 * @param atom Atom from which basic properties (everything but values) must be copied
	 * @param newValues New values
	 */
	public Atom (Atom atom, List<AtomArgument> newValues, AtomType type)
	{
		Peer peer = atom.getPeer();
		Schema schema = atom.getSchema();
		Relation relation = atom.getRelation();
		boolean mapping = atom.isMapping();
		_relationContext = new RelationContext (relation, schema, peer, mapping);
		_skolemKeyValues = new ArrayList<AtomArgument>();
		if(atom.getSkolemKeyVals() == null)
			_skolemKeyValues = null;
		else
			_skolemKeyValues = new ArrayList<AtomArgument>();		
		
		int i = 0;
		for (AtomArgument val : newValues){
			AtomArgument vc = val.deepCopy();
			_values.add (vc);
			_isNullable.add(atom._isNullable.get(i++));
			
			if(atom.getSkolemKeyVals() != null	&& atom.getSkolemKeyVals().contains(val))
				_skolemKeyValues.add (vc);
		}
		
		_type = type;
		_neg = atom.isNeg();
		_del = atom.getDel();
		_allStrata = atom.allStrata();
		
		_annotations.addAll(atom.getAnnotations());
		
		setValuesTypes();
		
	}
	
	public Atom (Atom atom, List<AtomArgument> newValues)
	{
//		this(atom, newValues, AtomType.NONE);
		this(atom, newValues, atom.getType());
	}
	
	public Atom (Atom atom, AtomType type)
	{
		this(atom, atom.getValues(), type);
	}
	
	
	public static String typeToSuffix(AtomType type){
		if(type == AtomType.NONE){
			return "";
		}else{
			return "_" + type.toString();
		}
	}
	
	public static AtomType getSuffix(String name) {
		int index = name.lastIndexOf("_");
		if (index != -1) {
			String suffix = name.substring(index+1);
			for (AtomType a : AtomType.values()) {
				if (suffix.equals(a.toString())) {
					return a;
				}
			}
		}
		return AtomType.NONE;
	}
	
	public static String getPrefix(String name) {
		AtomType a = getSuffix(name);
		if (a == AtomType.NONE) {
			return name;
		} else {
			return name.substring(0, name.length() - a.toString().length() - 1);
		}
	}

	public static String typeToString(AtomType type){
		if(type == AtomType.NONE){
			return "";
		}else{
			return type.toString();
		}
	}	
	
	public synchronized AtomType getType() 
	{
		return _type;
	}

	public synchronized void setType(AtomType type) 
	{
		_type = type;
	}
	
	/**
	 * Get the peer containing the schema/relation referenced by the mapping atom
	 * @return Peer referenced
	 */	
	public Peer getPeer() {
		return _relationContext.getPeer();
	}
	
	/**
	 * Get the schema containing the relation referenced by the mapping atom
	 * @return Schema referenced
	 */	
	public Schema getSchema() {
		return _relationContext.getSchema();
	}


	/**
	 * Check whether the relation for this atom is a mapping relation
	 * @return true, if relation is mapping relation, false otherwise
	 */	
	public boolean isMapping ()
	{
		return _relationContext.isMapping();
	}


	/**
	 * Get the relation referenced by the mapping atom
	 * @return Relation referenced
	 */	
	public Relation getRelation ()
	{
		return _relationContext.getRelation();		
	}
	
	/**
	 * Get the relation with it's full context (relation's schema and peer)
	 * @return
	 */
	public RelationContext getRelationContext ()
	{
		return _relationContext;
	}
	
	/** 
	 * Get the list of values for this mapping atom
	 * @return List of atom values
	 */
	public List<AtomArgument> getValues ()
	{
		return _values;
	}
	
	/** 
	 * Get the list of values for this mapping atom
	 * @return List of atom values
	 */
	public List<AtomVariable> getVariables ()
	{
		List<AtomVariable> l = new ArrayList<AtomVariable>();
		for(AtomArgument v : _values){
			if(v instanceof AtomVariable)
			l.add((AtomVariable)v);
		}
		return l;
	}
	
	public synchronized void setAllStrata ()
	{
		_allStrata = true;
	}

	public synchronized boolean allStrata ()
	{
		return _allStrata;
	}
	
	public synchronized void setNeg (boolean neg)
	{
		_neg = neg;
	}
	
	public void negate ()
	{
		setNeg(true);
	}
	
	public synchronized boolean isNeg ()
	{
		return _neg;
	}
	
	public synchronized void setDel (boolean del)
	{
		_del = del;
	}

	public synchronized boolean getDel ()
	{
		return _del;
	}
	
	public synchronized void setSkolemKeyVals (List<AtomArgument> vals)
	{
		_skolemKeyValues = vals;
	}

	public synchronized boolean isSkolem ()
	{
		return _skolemKeyValues != null;
	}
	
	public synchronized List<AtomArgument> getSkolemKeyVals(){
		return _skolemKeyValues;
	}

	public synchronized void deskolemizeAllVars(){
		for(AtomVariable var : getVariables()){
			if(var.isSkolem()){
				var.setSkolemDef(null);
			}
		}
	}
	
	/**
	 * Get a deep copy of the current atom when the whole system is being deep copied
	 * @param system Deep copy of the system containing the mapping.
	 * 				 Used to get find the peer/schema/relation referenced by the new mapping atom.
	 * @return Deep copy
	 * @see Atom#ScMappingAtom(Atom, OrchestraSystem)
	 */
	public synchronized Atom deepCopy (OrchestraSystem system)
	{
		return new Atom(this, system);
	}

	/**
	 * Get a deep copy of the current atom, but keeps references to the same relation.<BR>
	 * If the whole system is being deep copied, then call deepCopy(OrchestraSystem) to update
	 * the references to the relation (to use the new one)
	 * @return Deep copy
	 * @see Atom#ScMappingAtom(Atom)
	 * @see Atom#deepCopy(OrchestraSystem)
	 */
	public synchronized Atom deepCopy ()
	{
		return new Atom(this);
	}	

	/**
    * Returns a description of the mapping, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Mapping description Peer.Schema.RelName(variables)
    */
	@Override
	public synchronized String toString() {
		// TODO Auto-generated method stub
		
		StringBuffer buff = new StringBuffer ();
		buff.append(_relationContext.toString() + "(");
		boolean first = true;
		int inx = 0;
		for (AtomArgument value : getValues())
		{
			buff.append((first?"":",") + value.toString());
			if (isNullable(inx++))
				buff.append("*");
			first = false;
		}
		buff.append (")");
		String strRes = buff.toString();
		
		if (getType() != AtomType.NONE)
			strRes = getType().toString() + "_" + strRes;
		if (isNeg())
			strRes = "not(" + strRes + ")";
		return strRes;
	}
		
	/*
	 * 	 @return RelName(variables) - not used anywhere
	 */	
	public synchronized String toString2() {
		// TODO Auto-generated method stub
		StringBuffer buff = new StringBuffer ();
		buff.append(getRelation().getName());
		if (getType() != AtomType.NONE)
			buff.append("_" + getType().toString());
		buff.append("(");
		boolean first = true;
		for (AtomArgument value : getValues())
		{
			buff.append ((first?"":",") + value.toString());
			first = false;
		}

		buff.append(")");
		String strRes = buff.toString();
		if (isNeg())
			strRes = "not(" + strRes + ")";
		
		return strRes;
	}
		
/*
 *  @return Schema.RelName only
 */
	public synchronized String toString3() {
		// TODO Auto-generated method stub
		String strRes = getRelation().getFullQualifiedDbId();
		if (getType() != AtomType.NONE)
			strRes = strRes + "_" + getType().toString();
		return strRes;
	}

	/*
 * 	 @return RelName only
 */	
	public synchronized String toString4() {
		// TODO Auto-generated method stub
		String strRes = getRelation().getName();
		if (getType() != AtomType.NONE)
			strRes = strRes + "_" + getType().toString();
		return strRes;
	}

	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen atom will be returned, if false this atom will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public synchronized Atom fresh(Map<String, String> freshenVars, boolean asNewObject) {
		Atom atom;

		List<AtomArgument> newValues = new ArrayList<AtomArgument> ();
		for (AtomArgument val : getValues())
			newValues.add(val.fresh(freshenVars, asNewObject));
		
		if (asNewObject){
			atom = new Atom(this, newValues);

			atom.setType(getType());
			atom.setNeg(isNeg());
			atom.setDel(getDel());
			if(allStrata())
				atom.setAllStrata();
			return atom;
		}else{
			_values = newValues;

			return this;
		}
	}	

	
	/**
	 * Substitute all occurences of <code>valInit</code> with <code>valRepl</code>
	 * in the atom values
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */	
	public synchronized void substitute (AtomArgument oldVal, 
								AtomArgument newVal)
	{
		for (int i = 0 ; i < _values.size() ; i++)
		{
			AtomArgument value = _values.get(i);
			// If the atom value is to be replaced ...
			if (value.equals(oldVal) && oldVal.equals(value))
			{
				_values.remove(i);
				_values.add(i, newVal);
			}
			// Else replace any occurence of oldVal inside the atom sub-atoms if any
			else
				value.substitute(oldVal, newVal);
		}
	}
	
	public synchronized void renameVariables (String extension)
	{
		for (AtomArgument val : getValues())
			val.renameVariable(extension);
	}

	public synchronized void renameVariables (Map<String,String> renameTable)
	{
		for (AtomArgument val : getValues())
			val.renameVariable(renameTable);
	}
	
	public synchronized Map<String, AtomArgument> varHomomorphism(Atom other){
		HashMap<String, AtomArgument> varmap = new HashMap<String, AtomArgument> ();
		if(getRelationContext().equals(other.getRelationContext())){
			for(int i = 0; i < getValues().size(); i++){
				AtomArgument v = getValues().get(i);
				if(v instanceof AtomVariable){
					AtomVariable var = (AtomVariable)v;
					varmap.put(var.getName(), other.getValues().get(i));
				}else if(v instanceof AtomConst){
					AtomConst c = (AtomConst)v;
					AtomArgument v2 = other.getValues().get(i);
					if(v2 instanceof AtomConst){
						AtomConst c2 =(AtomConst)v2;
						if(!c2.equals(c)){
							return null;
						}
					}else{
						return null;
					}
				}else{
					return null;
				}
			}
			return varmap;
		}else{
			return null;
		}
	}
	
	public synchronized List<RuleEqualityAtom> varHomomorphismEq(Atom other) {
		List<RuleEqualityAtom> ret = new ArrayList<RuleEqualityAtom>();
		try{
		if(getRelationContext().equals(other.getRelationContext())){
			for(int i = 0; i < getValues().size(); i++){
				AtomArgument v = getValues().get(i);
				if(v instanceof AtomVariable){
					AtomVariable var = (AtomVariable)v;
					ret.add(new RuleEqualityAtom(var, other.getValues().get(i)));
				}else if(v instanceof AtomConst){
					AtomConst c = (AtomConst)v;
					AtomArgument v2 = other.getValues().get(i);
					if(v2 instanceof AtomConst){
						AtomConst c2 =(AtomConst)v2;
						if(!c2.equals(c)){
							return null;
						}
					}else{ // not quite a homomorphism, but necessary for unfolding of idbs
						ret.add(new RuleEqualityAtom(v, v2));
					}
				}else{
					return null;
				}
			}
			return ret;
		}else{
			return null;
		}
		}catch(CompositionException e){
			e.printStackTrace();
			return null;
		}
	}

	
	public synchronized void substituteVars(Map<String, AtomArgument> varmap){
    	
    	for(int i = 0; i < getValues().size(); i++){
    		AtomArgument v = getValues().get(i);
    	
    		if(v instanceof AtomVariable){
    			AtomVariable var = (AtomVariable)v;
    			if(varmap.containsKey(var.getName())){
    				getValues().remove(i);
    				getValues().add(i, varmap.get(var.getName()));
    			}
    		}else if (v instanceof AtomConst){ 
    			
    		} else { // Should do smth else for skolems 
    			throw new RuntimeException("Cannot substitute non-variable: " + v.toString());
    			//System.out.println(1/0);
    		}
    	}
    }
    
    public synchronized boolean samePattern(Atom other){
    	if((!getRelationContext().equals(other.getRelationContext())) || (getValues().size() != other.getValues().size()))
    		return false;	
    	
    	for(int i = 0; i < getValues().size(); i++) {
    		AtomArgument v = getValues().get(i);
    		AtomArgument ov = other.getValues().get(i);
    		
    		if(((v instanceof AtomVariable) && !(ov instanceof AtomVariable)) ||
    		   ((v instanceof AtomConst) && !(ov instanceof AtomConst))) {
    			return false;
    		} else if ((v instanceof AtomConst) && (ov instanceof AtomConst)) {
    			AtomConst vc = (AtomConst)v;
    			AtomConst ovc = (AtomConst)ov;
    			
    			if(!vc.equals(ovc))
    				return false;
    		} // else Skolem?
    	}
    	
    	return true;
    }
    
    public synchronized void renameExistentialVars(Mapping r){
    	for(AtomArgument val : getValues()){
    		if(val instanceof AtomVariable){
    			if(!r.isDistinguished((AtomVariable) val)){
    				((AtomVariable) val).setExistential(true);
    				((AtomVariable) val).setName(Mapping.getFreshAutogenVariableName());
    			}
    		}else if(val instanceof AtomConst){
    			// do nothing?
   			}else{
    			//System.out.println(1/0);
        		throw new RuntimeException("Cannot rename non-variable: " + val.toString());
    		}
    	}
    }
    
    public synchronized void serialize(Document doc, Element atom) {
    	StringBuffer buf = new StringBuffer(_relationContext.toString());
    	buf.append("(");
    	boolean first = true;
    	for (AtomArgument value : _values) {
    		if (first) {
    			first = false;
    		} else {
    			buf.append(", ");
    		}
    		buf.append(value.toString());
    	}
    	buf.append(")");
    	atom.setTextContent(buf.toString());
    }
    
    /**
	 * Returns an XML element representation of this {@code Atom}. It is more
	 * verbose than that produced by {@code serialize(Document, Element)}. 
	 * 
	 * @param doc only used to create {@code Element}s.
	 * @return an XML element representation of this {@code Atom}.
	 */
    public Element serializeVerbose(Document doc) {
    	Element e = doc.createElement("atom");
    	Element atomValue = doc.createElement("atomValue");
    	e.appendChild(atomValue);
    	serialize(doc, atomValue);
    	Element relationContext = _relationContext.serialize(doc);
    	e.appendChild(relationContext);
    	if (_skolemKeyValues != null && !_skolemKeyValues.isEmpty()) {
    		Element skolemKeyValues = doc.createElement("skolemKeyValues");
    		skolemKeyValues.setTextContent(_skolemKeyValues.toString());
    		e.appendChild(skolemKeyValues);
    	}
    	e.setAttribute("type", _type.toString());
    	e.setAttribute("negated", Boolean.toString(_neg));
    	e.setAttribute("backwardDeletable", Boolean.toString(_del));
    	//It seems like _allStrata cannot be set to false.
    	e.setAttribute("allStrata", Boolean.toString(_allStrata));
    	return e;
    }
    
	/**
	 * Returns the {@code Atom} represented by {@code atomElement}.
	 * 
	 * @param atomElement an {@code Element} returned by {@code
	 *            Atom.serializeVerbose()}
	 * @param counter
	 * @param system 
	 * @return the {@code Atom} represented by {@code atomElement}
	 * @throws XMLParseException
	 */
	public static Atom deserializeVerbose(Element atomElement,
			Holder<Integer> counter, OrchestraSystem system) throws XMLParseException {
		try {
			Element atomValue = DomUtils.getChildElementByName(atomElement,
					"atomValue");
			PositionedString pStr = new PositionedString(atomValue
					.getTextContent());

			RelationContext relationContext = RelationContext
					.deserialize(DomUtils.getChildElementByName(atomElement,
							"relationContext"), system);
			// Use this to get list of AtomArguemnt
			UntypedAtom u = UntypedAtom.parse(pStr, counter);
			AtomType type = AtomType.valueOf(atomElement.getAttribute("type"));
			Atom a = new Atom(relationContext, u.m_values, type);
			boolean isNegated = Boolean.parseBoolean(atomElement.getAttribute("negated"));
			a.setNeg(isNegated);
			
			boolean backwardDeletable = Boolean.parseBoolean(atomElement.getAttribute("backwardDeletable"));
			a.setDel(backwardDeletable);
			return a;
		} catch (ParseException e) {
			throw new XMLParseException(e);
		}
	}

	public static Atom deserialize(OrchestraSystem catalog, Element atom, Holder<Integer> counter) throws XMLParseException {
    	PositionedString str = new PositionedString(atom.getTextContent());
    	try {
    		UntypedAtom u = UntypedAtom.parse(str, counter);
    		return u.getTyped(catalog);
    	} catch (ParseException e) {
    		throw new XMLParseException(e);
    	} catch (RelationNotFoundException e) {
    		throw new XMLParseException(e);
    	} catch (Exception e) {
    		e.printStackTrace();
    		throw new XMLParseException("Error deserializing" + str.getString() + ": " + e.getMessage());
    	}
    }

//    public int hashCode() {
//    	int ret = this.toString().hashCode();
//    	return ret;
//    }
    
    public boolean equals(Object oth){
    	Atom other;
    	if(oth instanceof Atom)
    		other = (Atom)oth;
    	else
    		return false;

    	if(!this._relationContext.equals(other._relationContext))
    		return false;
    	List<AtomArgument> thisVals = getValues();
    	List<AtomArgument> otherVals = other.getValues();

    	if(thisVals.size() != otherVals.size())
    		return false;

    	for(int i = 0; i < thisVals.size(); i++){
    		if(!thisVals.get(i).equals(otherVals.get(i)))
    			return false;
    	}
    	
    	return true;
    }
    
    public boolean hasExistentialVariables(Mapping r){
    	for(AtomVariable v : getVariables()){
        	boolean found = false;
    		for(AtomVariable var : r.getAllBodyVariablesInPosAtoms()){
       			if(var.equals(v))
        			found = true;
        	}
    		if(!found)
    			return true;
    	}
    	return false;
    }
    
    public boolean isNullable(int inx) {
    	return _isNullable.get(inx).booleanValue();
    }
    
    public void setIsNullable(int inx) {
    	setIsNullable(inx, true);
    }
    
    public void clearIsNullable(int inx){
    	setIsNullable(inx, false);
    }
    
    public void setIsNullable(int inx, boolean status) {
    	_isNullable.set(inx, new Boolean(status));
    	getRelation().setIsNullable(inx, status);
    }
    
    public void replaceRelationContext(RelationContext newCx) {
    	_relationContext = newCx;
    }
    
    public void addArgument(AtomArgument a, boolean isNullable, boolean isAnnotation,
    		String label, Type dataType) throws BadColumnName {
    	getValues().add(a);
    	//getRelation().addCol(label, dataType)
    	_isNullable.add(isNullable);
    	_isAnnotated = _isAnnotated || isAnnotation;
//    	if (getRelation().getNumCols() < getValues().size())
//    		getRelation().addField(new RelationField(label, label, dataType));
    }
    
    public boolean isAnnotated() {
    	return _isAnnotated;
    }

	public List<AtomAnnotation> getAnnotations() {
		return _annotations;
	}
	
	public void setAnnotations(List<AtomAnnotation> ann) {
		_annotations = ann;
	}
	
	public void addAnnotation(AtomAnnotation ann) {
		_annotations.add(ann);
	}
}
