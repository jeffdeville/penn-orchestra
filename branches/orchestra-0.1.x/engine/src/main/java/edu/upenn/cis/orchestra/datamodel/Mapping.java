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
package edu.upenn.cis.orchestra.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomSkolem;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.exchange.RuleFieldMapping;
import edu.upenn.cis.orchestra.mappings.MappingsCompositionMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.RuleEqualityAtom;
import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Mapping definition
 * @author Olivier Biton, gkarvoun
 * ****************************************************************
 */
public class Mapping {
	protected List<RuleEqualityAtom> _eqAtoms=new ArrayList<RuleEqualityAtom> ();

	/** Mapping id, must be unique in the peer */
	private String _id;	
	/** Mapping description */
	private String _description;
	
	/** Used when a mapping is derived from a previous one */
	private String _derivedFrom = null;
	
	/** True if the mapping is materialized (replicate update), false if it must 
	 * only be used for "on the fly queries rewritings"
	 */
	private boolean _materialized;
	/** True if the mapping is bidirectional
	 */
	private boolean _bidirectional;
	/**
	 * Trust rank for this mapping
	 */
	private int _trustRank;
	/**
	 * List of head atoms
	 */
	protected List<Atom> _head = new Vector<Atom> ();
	/**
	 * List of body atoms
	 */
	protected List<Atom> _body = new Vector<Atom> ();
	
	private static int _nextAutogenVarFreshId=1;
	
	private RelationContext _provRel = null;
	
	protected boolean _fakeMapping = false;
	
	public Mapping()
	{
		_id = "-1";
		_description = "INTERNAL RULE";
		_materialized = true;
		_trustRank = 1;
	}
	
	public Mapping(Atom head, Atom body)
	{
		this();
		_head.add(head);
		_body.add(body);
	}
	
	public Mapping(Atom head, List<Atom> body)
	{
		this();
		_head.add(head);
		_body.addAll(body);
	}
	
	public Mapping(List<Atom> head, List<Atom> body)
	{
		this();
		_head.addAll(head);
		_body.addAll(body);
	}
	
	public void setId(String id) {
		_id = id;
	}

	/**
	 * Creates a new mapping
	 * @param id Mapping id, must be unique in the peer 
	 * @param description Mapping description
	 * @param isMaterialized True if the mapping if materialized (replicate update), false if it must 
	 * 					only be used for "on the fly queries rewritings"
	 * @param isBidirectional True if the mapping is bidirectional, false otherwise
	 * @param trustRank Trust rank for this mapping
	 * @param head List of head atoms
	 * @param body List of body atoms
	 */
	public Mapping (String id,
						String description,
						boolean isMaterialized,
						boolean isBidirectional,
						int trustRank,
						List<Atom> head,
						List<Atom> body)
	{
		this(id, description, isMaterialized, trustRank, head, body);
		_bidirectional = isBidirectional;
	}
	
	/**
	 * Creates a new mapping
	 * @param id Mapping id, must be unique in the peer 
	 * @param description Mapping description
	 * @param isMaterialized True if the mapping if materialized (replicate update), false if it must 
	 * 					only be used for "on the fly queries rewritings"
	 * @param trustRank Trust rank for this mapping
	 * @param head List of head atoms
	 * @param body List of body atoms
	 */
	public Mapping (String id,
						String description,
						boolean isMaterialized,
						int trustRank,
						List<Atom> head,
						List<Atom> body)
	{
		_id = id;
		_description = description;
		_materialized = isMaterialized;
		_trustRank = trustRank;
		_head.addAll(head);
		_body.addAll(body);
		
	}
	
	/**
	 * Deep copy of a given mapping. <BR>
	 * To benefit from polymorphism, use deepCopy method
	 * @param mapping Mapping to deep copy
	 * @param system Deep copy of the system containing the original mapping. Peers
	 * 				referenced in the new mapping being created must exist in this system 
	 * @see Mapping#deepCopy(OrchestraSystem)
	 * @see OrchestraSystem#deepCopy()
	 */
	protected Mapping (Mapping mapping, OrchestraSystem system)
	{
		_id = mapping.getId();
		_description = mapping.getDescription();
		_materialized = mapping.isMaterialized();
		_trustRank = mapping.getTrustRank();
		_fakeMapping = mapping.isFakeMapping();

		for (Atom atom : mapping.getMappingHead())
			_head.add(atom.deepCopy (system));
		for (Atom atom : mapping.getBody())
			_body.add(atom.deepCopy (system));
	}

	/**
	 * Deep copy of a given mapping, but keeping references to the same relation. <BR>
	 * To benefit from polymorphism, use deepCopy method. If the full system is being deep
	 * copied, use deepCopy method with OrchestraSystem as a parameter
	 * @param mapping Mapping to deep copy
	 * @see Mapping#deepCopy()
	 */
	protected Mapping (Mapping mapping)
	{
		_id = mapping.getId();
		_description = mapping.getDescription();
		_materialized = mapping.isMaterialized();
		_trustRank = mapping.getTrustRank();
		_fakeMapping = mapping.isFakeMapping();

		for (Atom atom : mapping.getMappingHead())
			_head.add(atom.deepCopy ());
		for (Atom atom : mapping.getBody())
			_body.add(atom.deepCopy ());
	}	
	
	//TODO Exception if peer ref or rel unknown

	/**
	 * Get the mapping description
	 * @return Mapping descriptino
	 */
	public synchronized String getDescription() {
		return _description;
	}

	/**
	 * Change the mapping description
	 * @param description New mapping description
	 */
	public synchronized void setDescription(String description) {
		this._description = description;
	}


	/**
	 * Get the mapping id
	 * @return Mapping id, must be unique in the peer 
	 */
	public String getId() {
		return _id;
	}
	
	/**
	 * Get the list of head atoms
	 * WARNING: This is not a deep copy
	 * @return List of head atoms
	 */
	public synchronized List<Atom> getMappingHead ()
	{
		return _head;
	}
	
	/**
	 * Copy the list of head atoms <BR>
	 * @return List of head atoms
	 */
	public synchronized List<Atom> copyMappingHead ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _head){
			b.add(a.deepCopy());
		}
		return b;
	}

	/**
	 * Get the list of body atoms <BR>
	 * WARNING: This is not a deep copy
	 * @return List of body atoms
	 */
	public synchronized List<Atom> getBody ()
	{
		return _body;
	}
	
	public synchronized void setBody (List<Atom> body)
	{
		_body = body;
	}
	
	/**
	 * Copy the list of body atoms <BR>
	 * @return List of body atoms
	 */
	public synchronized List<Atom> copyBody ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
			b.add(a.deepCopy());
		}
		return b;
	}
	

	/**
	 * Get the list of body atoms that are not "fake" skolem atoms <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> getBodyWithoutSkolems ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
			if(!a.isSkolem()){
				b.add(a);
			}
		}
		return b;
	}

	/**
	 * Get the list of deep copies of body atoms that are not "fake" skolem atoms <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> copyBodyWithoutSkolems ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
			if(!a.isSkolem()){
				b.add(a.deepCopy());
			}
		}
		return b;
	}

	
	/**
	 * Get the list of "fake" skolem atoms in the body of this mapping <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> getSkolemAtoms ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
			if(a.isSkolem()){
				b.add(a);
			}
		}
		return b;
	}
	/**
	 * Copy the list of "fake" skolem atoms in the body of this mapping <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> copySkolemAtoms ()
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
			if(a.isSkolem()){
				b.add(a.deepCopy());
			}
		}
		return b;
	}
	
	/**
	 * Get the list of "fake" skolem atoms in the body of this mapping,
	 * that "correspond" to the supplied variables <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> getSkolemAtomsForVars (List<AtomVariable> vars)
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
//			First param of skolem atom is the name of the corresponding variable in the target
			if(a.isSkolem() && vars.contains(a.getValues().get(0))){
				b.add(a);
			}
		}
		
		return b;
	}
	
	/**
	 * Copy the list of "fake" skolem atoms in the body of this mapping,
	 * that "correspond" to the supplied variables <BR>
	 * 	 * @return List of body atoms
	 */
	public synchronized List<Atom> copySkolemAtomsForVars (List<AtomVariable> vars)
	{
		List<Atom> b = new ArrayList<Atom>();
		for(Atom a : _body){
//			First param of skolem atom is the name of the corresponding variable in the target
			if(a.isSkolem() && vars.contains(a.getValues().get(0))){
				b.add(a.deepCopy());
			}
		}
		
		return b;
	}

	/**
	 * Add a new atom to the head, at position <code>rank</code>
	 * @param atom New atom
	 * @param rank Position in the head atoms
	 */
	public synchronized void addToHead (Atom atom, int rank)
	{
		_head.add(rank, atom);
	}
	
	/**
	 * Add a new atom to the head, at the end of the atoms list
	 * @param atom New atom
	 */	
	public synchronized void addToHead (Atom atom)
	{
		_head.add (atom);
	}
	
	/**
	 * Add a new atom to the head, at body<code>rank</code>
	 * @param atom New atom
	 * @param rank Position in the body atoms
	 */
	public synchronized void addToBody (Atom atom, int rank)
	{
		_body.add (rank, atom);
	}
	
	/**
	 * Add a new atom to the body, at the end of the atoms list
	 * @param atom New atom
	 */	
	public synchronized void addToBody (Atom atom)
	{
		_body.add(atom);
	}

	/**
	 * Know if this mapping is materialized
	 * @return True if the mapping is materialized (replicate update), false if it must 
	 * only be used for "on the fly queries rewritings"
	 */
	public boolean isMaterialized() {
		return _materialized;
	}
	
	/**
	 * @return True if the mapping is bidirectional 
	 */
	public boolean isBidirectional() {
		return _bidirectional;
	}
	

	public void setFakeMapping(boolean v){
		_fakeMapping = v;
	}

	public boolean isFakeMapping(){
		return _fakeMapping;
	}

	/**
	 * Get the trust rank for this mapping
	 * @return trust rank
	 */
	public synchronized int getTrustRank() {
		return _trustRank;
	}

	/**
	 * Set the trust rank for this mapping
	 * @param trustRank Trust rank 
	 */
	public synchronized void setTrustRank(int trustRank) {
		this._trustRank = trustRank;
	}
	
	
	/**
	 * Get a deep copy of this mapping when the whole system is being deep copied
	 * @param system This system must be a deep copy of the system which contained the 
	 * 				 original mapping (thus must contain all the referenced peers)
	 * @return Deep copy
	 * @see OrchestraSystem#deepCopy()
	 */
	protected synchronized Mapping deepCopy (OrchestraSystem system)
	{
		return new Mapping (this, system);
	}
	
	/**
	 * Get a deep copy of this mapping, but keeping references to the same relations.
	 * If the full system is being deep copied then the new relations need to be used, call
	 * deepCopy (OrchestraSystem)
	 * @return Deep copy
	 * @see Mapping#deepCopy(OrchestraSystem)
	 */
	public synchronized Mapping deepCopy ()
	{
		return new Mapping (this);
	}
	
	
	/**
    * Returns a description of the mapping , conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>. <BR>
    * @return Mapping description
    */	
	public synchronized String toString ()
	{
		StringBuffer buff = new StringBuffer ();

		buff.append(getId());
		buff.append(" " + (isMaterialized()?"MATERIALIZED":"VIRTUAL"));
		buff.append(": ");
		
		boolean first = true;
		for (Atom atom : getMappingHead())
		{
			buff.append((first?"":",") + atom.toString());
			
			first = false;
		}
		buff.append (" :- ");
		first = true;
		for (Atom atom : getBody())
		{
			buff.append((first?"":",") + atom.toString());
			first = false;
		}
		return buff.toString();
		
	}	
	
	public synchronized void renameVariables (String extension)
	{
		for (Atom atom : getMappingHead())
			atom.renameVariables(extension);
		for (Atom atom : getBody())
			atom.renameVariables(extension);
		
	}
	
	public synchronized void renameExistentialVars(){
		Map<String, String> renamedVarNames = new HashMap<String, String>();
		List<Atom> allAtoms = new ArrayList<Atom>();
		
		allAtoms.addAll(getMappingHead());
		allAtoms.addAll(getBody());
		
		for(Atom a : allAtoms){
			for(AtomArgument v : a.getValues()){
				if(v instanceof AtomVariable){
					AtomVariable val = (AtomVariable)v;
					if("-".equals(val.toString()) || "_".equals(val.toString())){
						val.setExistential(true);
						val.setName(Mapping.getFreshAutogenVariableName());
					} else if(!isDistinguished(val)){
						if(renamedVarNames.containsKey(val.getName())){
							val.setName(renamedVarNames.get(val.getName()));
						}else{
							val.setExistential(true);
							String newName = Mapping.getFreshAutogenVariableName();
							renamedVarNames.put(val.getName(), newName);
							val.setName(newName);
						}
					}
				}else if(v instanceof AtomConst){
					// do nothing?
				}else{
					//System.out.println(1/0);
					throw new RuntimeException("Cannot rename non-variable: " + v.toString());
				}
			}
		}
	}
	
	public synchronized List<AtomVariable> getAllListVariables(List<Atom> l){
		List<AtomVariable> allVars = new ArrayList<AtomVariable>();
		Set<String> allVarNames = new HashSet<String> ();

		for (Atom atom : l)
			if(!atom.isSkolem()){
				for(AtomArgument val : atom.getValues())
				{
					// ignore constants ... and there shouldn't be any skolems in the body anyway
					if (val instanceof AtomVariable && !allVarNames.contains(val.toString())) { 
						allVars.add((AtomVariable) val.deepCopy());
						allVarNames.add (val.toString());
					}
					else
						if (val instanceof AtomSkolem)
							assert false : "THERE SHOULDN'T BE ANY SKOLEM IN THE BODY FOR THIS FIRST IMPLEMENTATION";
				}
			}
		return allVars;
	}

	public synchronized List<AtomArgument> getAllListArgs(List<Atom> l){
		List<AtomArgument> allVars = new ArrayList<AtomArgument>();
		Set<String> allVarNames = new HashSet<String> ();

		for (Atom atom : l)
			if(!atom.isSkolem()){
				for(AtomArgument val : atom.getValues())
				{
					// ignore constants ... and there shouldn't be any skolems in the body anyway
					if ((val instanceof AtomVariable || val instanceof AtomConst) && !allVarNames.contains(val.toString())) { 
						allVars.add(val.deepCopy());
						allVarNames.add (val.toString());
					}
					else
						if (val instanceof AtomSkolem)
							assert false : "THERE SHOULDN'T BE ANY SKOLEM IN THE BODY FOR THIS FIRST IMPLEMENTATION";
				}
			}
		return allVars;
	}

	public synchronized List<AtomArgument> getAllBodyArgs(){
		return getAllListArgs(getBody());
	}

	public synchronized List<AtomArgument> getAllHeadArgs(){
		return getAllListArgs(getMappingHead());
	}

	public synchronized List<AtomArgument> getAllMappingArgs(){
		List<Atom> hb = new ArrayList<Atom>();
		hb.addAll(getMappingHead());
		hb.addAll(getBody());
		return getAllListArgs(hb);
	}

	public synchronized List<AtomVariable> getAllHeadVariables(){
		return getAllListVariables(getMappingHead());
	}
	    
	public synchronized List<AtomVariable> getAllBodyVariables(){
		return getAllListVariables(getBody());
	}
	
	public synchronized List<AtomVariable> getAllBodyVariablesInPosAtoms(){
		List<Atom> posAtoms = new ArrayList<Atom>();
		for(Atom a : getBody()){
			if(!a.isNeg()){
				posAtoms.add(a);
			}
		}
		return getAllListVariables(posAtoms);
	}
	
	public synchronized List<AtomVariable> getAllMappingVariables(){
		List<Atom> hb = new ArrayList<Atom>();
		hb.addAll(getMappingHead());
		hb.addAll(getBody());
		return getAllListVariables(hb);
	}
	
	/**
	 * Get the mapping's equalities atoms (define equality between two variables).<BR>
	 * Note: this is not a deep copy
	 * @return
	 */
	public List<RuleEqualityAtom> getEqAtoms ()
	{
		return _eqAtoms;
	}
	
	public synchronized  boolean isDistinguished(AtomVariable v){
    	for(Atom at : getMappingHead()){
    		for(AtomArgument val : at.getValues())
    			if(val.equals(v))
    				return true;
    	}
    	return false;
    }
	
	public synchronized boolean isJoinVar(AtomVariable v){
    	int occurences = 0;
		for(Atom at : getBody()){
    		for(AtomArgument val : at.getValues())
    			if(val.equals(v) && !(at.getRelation() instanceof ProvenanceRelation))
    				occurences++;
    	}
		
    	return (occurences > 1);
    }
    
	/**
	 * Get a fresh variable name, that can't conflict with any other variable name.
	 * <b>Note:</b> this supposes that user variables are renamed with a "_" in the variable name
	 * to separate user variable namespace from automatically generated variables. This is done during 
	 * the inversion process (MappingsInversionMgt.inverseMapping) 
	 * 
	 * @return Fresh variable name
	 * @see MappingsCompositionMgt
	 */
    public static String getFreshAutogenVariableName() {
        return "X" + _nextAutogenVarFreshId++;
    }    

    protected static void serializeAtoms(Document doc, Element list, List<Atom> atoms) {
    	for (Atom atom : atoms) {
    		Element a = DomUtils.addChild(doc, list, "atom");
    		atom.serialize(doc, a);
    	}
    }
    
    /**
     * Appends verbose XML representations of each {@code Atom} in {@code atoms} to {@code list}.
     * 
     * @param doc only used to create {@code Element}s.
     * @param list
     * @param atoms
     */
    private static void serializeVerboseAtoms(Document doc, Element list, List<Atom> atoms) {
    	for (Atom atom : atoms) {
    		list.appendChild(atom.serializeVerbose(doc));
    	}
    }
    
	public synchronized void serialize(Document doc, Element mapping) {
    	mapping.setAttribute("name", _id);
    	mapping.setAttribute("description", _description);
    	mapping.setAttribute("materialized", _materialized ? "true" : "false");
    	Element head = DomUtils.addChild(doc, mapping, "head");
    	Element body = DomUtils.addChild(doc, mapping, "body");
    	serializeAtoms(doc, head, _head);
    	serializeAtoms(doc, body, _body);
    }
    
	/**
	 * Returns an XML element representation of this {@code Mapping}. It is more
	 * verbose than that produced by {@code serialize(Document, Element)}. 
	 * 
	 * @param doc only used to create {@code Element}s.
	 * @return an XML element representation of this {@code Mapping}.
	 */
    public Element serializeVerbose(Document doc) {
    	Element m = doc.createElement("mapping");
    	m.setAttribute("type", "mapping");
    	m.setAttribute("name", _id);
    	m.setAttribute("description", _description);
    	m.setAttribute("materialized", _materialized ? "true" : "false");
    	if (_derivedFrom != null) {
    		m.setAttribute("derivedFrom", _derivedFrom);
    	}
    	m.setAttribute("bidirectional", Boolean.toString(_bidirectional));
    	m.setAttribute("trustRank", Integer.toString(_trustRank));
    	m.setAttribute("isFakeMapping", Boolean.toString(_fakeMapping));
    	
    	Element head = DomUtils.addChild(doc, m, "head");
    	Element body = DomUtils.addChild(doc, m, "body");
    	serializeVerboseAtoms(doc, head, _head);
    	serializeVerboseAtoms(doc, body, _body);
    	if (_provRel != null) {
    		Element provRel = DomUtils.addChild(doc, m, "provenanceRelation");
    		provRel.appendChild(_provRel.serialize(doc));
    	}
    	if (!_eqAtoms.isEmpty()) {
    		Element eqAtoms = DomUtils.addChild(doc, m, "eqAtoms");
    		for (RuleEqualityAtom rea : _eqAtoms) {
    			eqAtoms.appendChild(rea.serialize(doc));
    		}
    	}
    	return m;
    }
    
	/**
	 * Returns an XML element representation of {@code mappings} using {@code
	 * serializeVerbose()} for the individual {@code mapping}s.
	 * 
	 * @param doc only used to create {@code Element}s.
	 * @param mappings
	 * @param elementName
	 * @return an XML element representation of {@code mappings}.
	 */
    public static Element serializeVerbose(Document doc, List<? extends Mapping> mappings, String elementName) {
    	Element list = doc.createElement(elementName);
    	for (Mapping mapping : mappings) {
    		list.appendChild(mapping.serializeVerbose(doc));
    	}
    	return list;
    }
//    public synchronized List<RuleFieldMapping> getFieldMappingForAtoms(List<Atom> atoms, List<String> indexFields){
//		List<RuleFieldMapping> fields = new ArrayList<RuleFieldMapping>();
//		final List<AtomVariable> allVars = getAllListVariables(atoms);
//
//    
//    }
    
	public List<RuleFieldMapping> getAppropriateRuleFieldMapping() throws IncompatibleTypesException{
		final List<RelationField> rfmIndexes = new ArrayList<RelationField> ();
		return(getAppropriateRuleFieldMapping(rfmIndexes));
	}
	
	public List<RuleFieldMapping> getAppropriateRuleFieldMapping(List<RelationField> rfmIndexes) throws IncompatibleTypesException{
		List<RuleFieldMapping> rf;
		
		rf = getRuleFieldMapping(rfmIndexes, true);
		
		return rf;
	}
    
    /**
	 * Takes the rule and returns a list of RuleMappings (one per attribute) for
	 * the corresponding schema.
	 * 
	 * @param rule
	 * @param fields
	 * @param indexFields
     * @throws IncompatibleTypesException 
	 */
	public synchronized List<RuleFieldMapping> getRuleFieldMapping(List<RelationField> indexFields, boolean includeConstants) throws IncompatibleTypesException
	{
		List<RuleFieldMapping> fields = new ArrayList<RuleFieldMapping>();
		final List<AtomArgument> allArgs;
		if(includeConstants){
			allArgs = getAllMappingArgs();
		}else{
			allArgs = new ArrayList<AtomArgument>();
			allArgs.addAll(getAllMappingVariables());
		}

		// Assume for now that the datatypes in the mappings are coherent. The first column 
		// found for each variable will be used to choose the type in the provenance relation
		// Assume also that the labeled null columns exist
		// Creates the provenance relation in the same schema as the head of the atom...

		final Set<String> srcRelations = new HashSet<String> ();
		Set<Integer> indexes = new HashSet<Integer> (); 
		List<String> srcColumns = new ArrayList<String>();
		final List<RelationField> allSrcFields = new ArrayList<RelationField>();
		List<RelationField> srcFields = new ArrayList<RelationField>();
		List<String> trgColumns = new ArrayList<String>();
		final List<RelationField> allTrgFields = new ArrayList<RelationField>();
		List<RelationField> trgFields = new ArrayList<RelationField>();
		final Set<String> trgRelations = new HashSet<String> ();
		final List<String> checkedFields = new ArrayList<String>();
		int j = 0;
		int k = 0;
		
		for (final AtomArgument var : allArgs)
		{
			if(!checkedFields.contains(var.toString())){
				// Get the target columns from the head of the rule
				boolean fndTrg = findArgInAtoms(var, getMappingHead(), allTrgFields, trgFields, trgColumns, trgRelations, 
						indexes, true);
				boolean indTrg = (indexes.size() != 0);

				// Get the source columns from the body of the rule
				boolean fndSrc = findArgInAtoms(var, getBody(), allSrcFields, srcFields, srcColumns, srcRelations, 
						indexes, true);
				boolean indSrc = (indexes.size() != 0);

				try {
					
					if(fndSrc){
						RuleFieldMapping m = new RuleFieldMapping(new 
								RelationField("C" + j, "C" + j, true, allSrcFields.get(j).getSQLTypeName()),
								srcFields, trgFields, var, (indSrc || indTrg), this);
//								srcFields, trgColumns, var, (indexes.size() != 0), this);
//								srcColumns, trgColumns, var, (indexes.size() != 0), this);
						fields.add (m);
						j++;
						if(fndTrg)
							k++;
					}else if(fndTrg){
						RuleFieldMapping m = new RuleFieldMapping(new 
								RelationField("CH" + k, "CH" + k, true, allTrgFields.get(k).getSQLTypeName()),
								srcFields, trgFields, var, (indTrg), this);
//								trgFields, trgColumns, var, (indexes.size() != 0), this);
//								trgColumns, trgColumns, var, (indexes.size() != 0), this);
						fields.add (m);
						k++;
					}
					
					
				} catch (UnsupportedTypeException ute) {
					// Should never happen
					ute.printStackTrace();
				}

				srcColumns = new ArrayList<String>();
				trgColumns = new ArrayList<String>();
				srcFields = new ArrayList<RelationField>();
				trgFields = new ArrayList<RelationField>();
				indexes = new HashSet<Integer>();
				
				checkedFields.add(var.toString());
			}
		}
	
		// Add the index sources
		for (final Integer index : indexes)
			indexFields.add (fields.get(index).outputField);
		
		return fields;
	}

	/**
	 * Takes the rule and returns a list of RuleMappings (one per attribute) for
	 * the corresponding schema.
	 * 
	 * @param rule
	 * @param fields
	 * @param indexFields
	 * @throws IncompatibleTypesException 
	 */
	public synchronized  List<RuleFieldMapping> getBodyFieldMapping(List<RelationField> indexFields, boolean includeConstants) throws IncompatibleTypesException
	{
		final List<AtomArgument> allArgs;
		if(includeConstants){
			allArgs = getAllBodyArgs();
		}else{
			allArgs = new ArrayList<AtomArgument>();
			allArgs.addAll(getAllBodyVariables());
		}
		List<RuleFieldMapping> fields = new ArrayList<RuleFieldMapping>();
	
		// Assume for now that the datatypes in the mappings are coherent. The first column 
		// found for each variable will be used to choose the type in the provenance relation
		// Assume also that the labeled null columns exist
		// Creates the provenance relation in the same schema as the head of the atom...
	
		final Set<String> srcRelations = new HashSet<String> ();
		Set<Integer> indexes = new HashSet<Integer> (); 
		List<String> srcColumns = new ArrayList<String>();
		final List<RelationField> allSrcFields = new ArrayList<RelationField>();
		List<RelationField> srcFields = new ArrayList<RelationField>();
		List<String> trgColumns = new ArrayList<String>();
		final List<RelationField> allTrgFields = new ArrayList<RelationField>();
		List<RelationField> trgFields = new ArrayList<RelationField>();
		final Set<String> trgRelations = new HashSet<String> ();
		int j = 0;
		for (final AtomArgument var : allArgs)
		{
			// Get the target columns from the head of the rule
			findArgInAtoms(var, getMappingHead(), allTrgFields, trgFields, trgColumns, trgRelations, 
					indexes, true);
			
			// Get the source columns from the body of the rule
			findArgInAtoms(var, getBody(), allSrcFields, srcFields, srcColumns, srcRelations, 
					indexes, true);
			boolean indSrc = (indexes.size() != 0);
			
			try {
				RuleFieldMapping m = new RuleFieldMapping(new 
						RelationField("C" + j, "C" + j, true, allSrcFields.get(j).getSQLTypeName()),
						srcFields, trgFields, var, indSrc, this);
//						srcFields, trgColumns, var, (indexes.size() != 0), this);
//						srcColumns, trgColumns, var, (indexes.size() != 0), this);
				
				fields.add (m);
			} catch (UnsupportedTypeException ute) {
				// Should never happen
				ute.printStackTrace();
			}
	
			srcColumns = new ArrayList<String>();
			trgColumns = new ArrayList<String>();
			srcFields = new ArrayList<RelationField>();
			trgFields = new ArrayList<RelationField>();
			indexes = new HashSet<Integer>();
			j++;
		}
	
		// Add the index sources
		for (final Integer index : indexes)
			indexFields.add (fields.get(index).outputField);
		
		return fields;
	}

	public static boolean checkIfVarInKey (final AtomVariable var,
			final List<Atom> atoms,
			final List<RelationField> srcFields,
			final List<String> srcColumns,
			final Set<String> srcRelations,
			final Set<Integer> indexes)
	{
		boolean fnd = false;
		final Iterator<Atom> itAtoms = atoms.iterator();
		while (itAtoms.hasNext() && (!fnd))
		{
			final Atom atom = itAtoms.next();
			final Iterator<AtomArgument> itVals = atom.getValues().iterator();
			int ind = 0;
			while (itVals.hasNext() && (!fnd))
			{
				final AtomArgument val = itVals.next();
				if (val.equals(var))
				{
					Relation rel = atom.getRelation();
					PrimaryKey key = rel.getPrimaryKey();
					RelationField f = rel.getField(ind);
					if(key.getFields().contains(f)){
						indexes.add(Integer.valueOf(srcColumns.size()-1));
						fnd = true;
					}
				}
				ind++;
			}
		}
		return fnd;
	}

	public static boolean findArgInAtoms (final AtomArgument var,
			final List<Atom> atoms,
			final List<RelationField> allSrcFields,
			final List<RelationField> srcFields,
			final List<String> srcColumns,
			final Set<String> srcRelations,
			final Set<Integer> indexes,
			boolean fndAll) throws IncompatibleTypesException 
	{
		boolean fnd = false;
		
		final Iterator<Atom> itAtoms = atoms.iterator();
		while (itAtoms.hasNext() && (fndAll || !fnd))
		{
			final Atom atom = itAtoms.next();
			if(!atom.isSkolem()){
				final Iterator<AtomArgument> itVals = atom.getValues().iterator();
				int ind = 0;
				while (itVals.hasNext() && (fndAll || !fnd))
				{
					final AtomArgument val = itVals.next();
					if (val.equals(var))
					{
						RelationField f = atom.getRelation().getField(ind);
//						This always adds the first found field
//						Change if we want to add, e.g., the "largest"
						if(!fnd){
							fnd = true;
							allSrcFields.add (f);
						}else{
							Type addedType = allSrcFields.get(allSrcFields.size()-1).getType();
							Type newType = f.getType();

							boolean addedTypeGood = newType.canPutInto(addedType);
							boolean newTypeGood = newType.canReadFrom(addedType);

//							Debug.println("OLD TYPE: " + addedType);
//							Debug.println("NEW TYPE: " + newType);

							if(!addedTypeGood && !newTypeGood){
								throw new IncompatibleTypesException(newType.toString(), addedType.toString());
							}else if(newTypeGood){
								allSrcFields.remove(allSrcFields.size()-1);
								allSrcFields.add(f);
							}
						}

						srcFields.add(atom.getRelation().getField(ind));
						srcColumns.add(/*atom.getRelation().getDbSchema() + "_" +*/ 
								atom.getRelation().getDbRelName() + "." + 
								atom.getRelation().getField(ind).getName());
						srcRelations.add(atom.getRelation().getFullQualifiedDbId() + " " + //" " + 
								/*atom.getRelation().getDbSchema() + "_" + */ 
								atom.getRelation().getDbRelName());

						if(atom.getRelation().getPrimaryKey().getFields().contains(f)){
							indexes.add(Integer.valueOf(srcColumns.size()-1));
						}else{

						}
					}
					ind++;
				}
			}
		}
		
		return fnd;
	}

	public static boolean findArgInAtoms (final AtomArgument var,
			final Atom atom,
			final List<RelationField> allSrcFields,
			final List<RelationField> srcFields,
			final List<String> srcColumns,
			final Set<String> srcRelations,
			final Set<Integer> indexes,
			boolean fndAll) throws IncompatibleTypesException 
	{
		final List<Atom> atoms = new ArrayList<Atom> (1);
		atoms.add(atom);
		return findArgInAtoms(var, atoms, allSrcFields, srcFields, srcColumns, srcRelations, indexes,
				fndAll);
	}

	protected static List<Atom> deserializeAtoms(OrchestraSystem catalog, Element parent, Holder<Integer> counter) 
    		throws XMLParseException { 
    	List<Atom> list = new ArrayList<Atom>();
    	for (Element atom : DomUtils.getChildElementsByName(parent, "atom")) {
    		Atom a = Atom.deserialize(catalog, atom, counter);
    		boolean del = Boolean.parseBoolean(atom.getAttribute("del"));
    		a.setDel(del);
    		list.add(a);
    	}
    	return list;
    }
    
    public static Mapping deserialize(OrchestraSystem catalog, Element mapping) throws XMLParseException {
    	String name = mapping.getAttribute("name");
    	String description = mapping.getAttribute("description");
    	boolean mat = Boolean.parseBoolean(mapping.getAttribute("materialized"));
    	boolean bidir = Boolean.parseBoolean(mapping.getAttribute("bidirectional"));
    		
    	Element h = DomUtils.getChildElementByName(mapping, "head");
    	Element b = DomUtils.getChildElementByName(mapping, "body");
    	if (h == null) {
    		throw new XMLParseException("Mapping is missing head element", mapping);
    	} else if (b == null) {
    		throw new XMLParseException("Mapping is missing body element", mapping);
    	}
    	Holder<Integer> counter = new Holder<Integer>(0);
    	List<Atom> head = deserializeAtoms(catalog, h, counter);
    	List<Atom> body = deserializeAtoms(catalog, b, counter);
    	return new Mapping(name, description, mat, bidir, 1, head, body);
    }
    
    public RelationContext getProvenanceRelation(){
    	return _provRel;
    }

    public void setProvenanceRelation(RelationContext rel){
    	_provRel = rel;
    	if (_provRel != null && !(_provRel.getRelation() instanceof ProvenanceRelation))
    		throw new RuntimeException("Illegal state: non-provenance relation: " + rel.getRelation().toString() + "!");
    }
    
    public Mapping composeWith (int atomIndex, Rule rule)
    throws CompositionException
    {
    	assert(getBody().get(atomIndex).getRelation() == rule.getHead().getRelation()) : 
    		"Impossible to compose with different relations in the atoms!";
//  	Freshen the mapping to compose with; to avoid variables confusions between the two mappings
    	rule = rule.fresh(true);

//  	Prepare the new body: atom at atomIndex replaced by freshen rule body
    	List<Atom> newBody = new ArrayList<Atom> ();
    	newBody.addAll(getBody());
    	Atom removedAtom = newBody.get(atomIndex);
    	newBody.remove(atomIndex);
    	int indAtom=0;
    	for (Atom atom : rule.getBody())
    		newBody.add(atomIndex + indAtom++, atom);

//  	Create the new Rule
    	Mapping res = new Mapping (getMappingHead(), newBody);
    	
    	res.setDerivedFrom(rule.getDerivedFrom());
    	res.setId(rule.getId());

//  	Copy the existing equalities atoms
    	for (RuleEqualityAtom atom : getEqAtoms())
    		res.addEqAtom(atom);

//  	Add the equalities atoms to define equalities between the initial mapping variables
//  	and the new composed mapping variables
    	for (int i = 0 ; i < removedAtom.getValues().size() ; i++)
    		res.addEqAtom(new RuleEqualityAtom(removedAtom.getValues().get(i), rule.getHead().getValues().get(i)));

    	return res;
    }

	public void addEqAtom (RuleEqualityAtom atom)
	{
		_eqAtoms.add (atom);
	}
	
	public void eliminateEqualities ()
	{
		for (int i = 0 ; i < getEqAtoms().size() ; i++)
		{
			// Get into val1 an atomValue that should be 
			// substituted with val2
			RuleEqualityAtom eqAtom = getEqAtoms().get(i);
			AtomArgument val1 = eqAtom.getVal1();
			AtomArgument val2 = eqAtom.getVal2();
			assert (val1.couldBeSubstitutedWith(val2)
						|| val2.couldBeSubstitutedWith(val1));
			
			// An equality between two skolems is actually an equality between the 
			// skolems parameters one to one (f(x,y)=f(u,v) iff x=u and y=v)
			if (val1 instanceof AtomSkolem && val2 instanceof AtomSkolem)
			{
				Iterator<AtomArgument> itParam1 = ((AtomSkolem) val1).getParams().iterator();
				Iterator<AtomArgument> itParam2 = ((AtomSkolem) val2).getParams().iterator();
				while (itParam1.hasNext())
					try
					{
						addEqAtom(new RuleEqualityAtom(itParam1.next(), itParam2.next()));
					} catch (CompositionException ex)
					{
						assert(false) : "Composition exception should not happen while removing equalities, compatibilities already checked when creating equalities!";
					}
			}
			else
			{
				if (!val1.couldBeSubstitutedWith(val2))
				{
					val1 = val2;
					val2 = eqAtom.getVal1();
				}
				
				// Run the substitution for all the atoms
				for(Atom a : getMappingHead())
					a.substitute(val1, val2);
				for (Atom atom : getBody())
					atom.substitute(val1, val2);
			}
		}
		_eqAtoms.clear();
	}

	public void setDerivedFrom(String s) {
		_derivedFrom = s;
	}
	
	public String getDerivedFrom() {
		return _derivedFrom;
	}

	/**
	 * Returns a list of {@code Atoms} as represented in {@code atom}.
	 * 
	 * @param atoms a "head" or "body" {@code Element} produced as part of
	 *            {@code serializeVerbose()}.
	 * @param system 
	 * @return
	 * @throws XMLParseException
	 */
	private static List<Atom> deserializeVerboseAtoms(Element atoms, OrchestraSystem system)
			throws XMLParseException {
		List<Atom> atomList = OrchestraUtil.newArrayList();
		List<Element> atomElementList = DomUtils.getChildElementsByName(atoms,
				"atom");
		Holder<Integer> counter = new Holder<Integer>(Integer.valueOf(0));
		for (Element atomElement : atomElementList) {
			atomList.add(Atom.deserializeVerbose(atomElement, counter, system));
		}
		return atomList;
	}

	/**
	 * Returns the {@code Mapping} represented by {@code mapping}. This is the
	 * appropriate method to use when you are unsure if {@code mapping}
	 * represents a {@code Mapping} or a {@code Rule}. If you know that {@code mapping}
	 * represents  {@code Rule}, then you can use {@code Rule.deserializeVerboseRule(Element)}.
	 * 
	 * @param mapping
	 * @param system 
	 * @return the {@code Mapping} represented by {@code mapping}
	 * @throws XMLParseException
	 */
	public static Mapping deserializeVerboseMapping(Element mapping, OrchestraSystem system)
			throws XMLParseException {
		Mapping result;
		String type = mapping.getAttribute("type");
		if ("mapping".equals(type)) {
			result = deserializeVerboseMappingImpl(mapping, system);
		} else if ("rule".equals(type)) {
			result = Rule.deserializeVerboseRule(mapping, system);
		} else if ("".equals(type)) {
			throw new XMLParseException("Missing 'type' attribute of 'mapping' element.");
		} else {
			throw new XMLParseException("Unrecognized Mapping type: ["
					+ type + "].");
		}
		return result;
	}

	/**
	 * Returns the {@code Mapping} represented by {@code mapping}.
	 * 
	 * @param mapping
	 * @param system 
	 * @return the {@code Mapping} represented by {@code mapping}
	 * @throws XMLParseException
	 */
	protected final static Mapping deserializeVerboseMappingImpl(Element mapping, OrchestraSystem system)
			throws XMLParseException {
		try {
			String name = mapping.getAttribute("name");
			String description = mapping.getAttribute("description");
			boolean mat = Boolean.parseBoolean(mapping
					.getAttribute("materialized"));
			boolean bidir = Boolean.parseBoolean(mapping
					.getAttribute("bidirectional"));
			int trustRank = Integer.parseInt(mapping.getAttribute("trustRank"));
			List<Atom> head = deserializeVerboseAtoms(DomUtils
					.getChildElementByName(mapping, "head"), system);
			List<Atom> body = deserializeVerboseAtoms(DomUtils
					.getChildElementByName(mapping, "body"), system);
			Mapping result = new Mapping(name, description, mat, bidir,
					trustRank, head, body);
			inializeFromElement(mapping, result, system);
			return result;
		} catch (XMLParseException e) {
			throw e;
		} catch (Exception e) {
			throw new XMLParseException(e);
		}
	}

	/**
	 * Sets some of {@code mapping}'s attributes to match those of {@code template}.
	 * Some of {@code Mapping}'s attributes are not handled by the constructors.
	 * So we handle them here for {@code deserializeVerboseMapping(Element)} and subclasses.
	 * 
	 * @param template an {@code Element} produced by {@code serialize(Document)}.
	 * @param mapping
	 * @param system 
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 * @throws CompositionException
	 */
	protected final static void inializeFromElement(Element template, Mapping mapping, OrchestraSystem system)
			throws XMLParseException, UnsupportedTypeException,
			CompositionException {
		mapping.setFakeMapping(Boolean.parseBoolean(template
				.getAttribute("isFakeMapping")));
		String derivedFrom = template.getAttribute("derivedFrom");
		// getAttribute() returns "" for non-existent attributes.
		if (!"".equals(derivedFrom)) {
			mapping.setDerivedFrom(derivedFrom);
		}
		Element provRel = DomUtils.getChildElementByName(template,
				"provenanceRelation");
		if (provRel != null) {
			Element relationContext = DomUtils.getChildElementByName(provRel,
					"relationContext");
			mapping.setProvenanceRelation(RelationContext
					.deserialize(relationContext, system));
		}

		Element eqAtomsElement = DomUtils.getChildElementByName(template,
				"eqAtoms");
		if (eqAtomsElement != null) {
			List<Element> eqAtomsList = DomUtils.getChildElementsByName(
					eqAtomsElement, "ruleEqualityAtom");
			for (Element eqAtomElement : eqAtomsList) {
				mapping.addEqAtom(RuleEqualityAtom.deserialize(eqAtomElement));
			}
		}
	}

}
