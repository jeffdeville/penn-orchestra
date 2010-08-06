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




import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.XMLParseException;

/****************************************************************
 * Mapping definition : Mapping atom values // variable
 * Note: Vars do not have to be the same objects in the head and 
 * body, the <code>equals</code> method will be used 

 * @author Olivier Biton
 *****************************************************************
 */
public class AtomVariable extends AtomArgument {

	/** Variable name **/
	private String _name;
	/** Static variable used to freshen vars **/
	private static long _nextFreshId=1;
	
	private Atom _skolemDef = null;
	
	private boolean _existential = false;
	
	/**
	 * Creates a new atom value of type variable
	 * @param name Variable name
	 */
	public AtomVariable (String name)
	{
		if (name.equals("_")){
			_name = Mapping.getFreshAutogenVariableName();
			_existential = true;
		}else
			_name = name;
		_skolemDef = null;
	}

	/**
	 * Creates a deep copy of an atom value of type variable. <BR>
	 * To benefit from polymorphism, use <code>deepCopy()</code>
	 * @param var Variable to copy
	 * @see AtomVariable#deepCopy()
	 */	
	protected AtomVariable (AtomVariable var)
	{
		super (var);
		_name = var.getName();
		_skolemDef = var.skolemDef();
		_existential = var.isExistential();
	}
	
	public synchronized String getName() {
		return _name;
	}

	public synchronized void setName (String name)
	{
		_name = name;
	}
	
	
	
	/**
	 * True if the other atom value is also a variable and has the same name
	 * @return True if variables are equivalent
	 */
	@Override
	public synchronized boolean equals(AtomArgument atomVal) {
		if (atomVal instanceof AtomVariable)
			return // super.equals(atomVal) && 
			       _name.equals(((AtomVariable) atomVal).getName());
		else
			return false;
	}


	public int hashCode() {
		return _name.hashCode();
	}
	
	/**
	 * Get string representation
	 * @return String representation 
	 */
	public synchronized String toString ()
	{
		return getName();
	}

	/**
	 * Get a deep copy
	 * @return Deep copy
	 */
	public synchronized AtomArgument deepCopy() {
		return new AtomVariable(this);
	}

	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition. <BR>
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen variable
	 */
	public synchronized AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		if (freshenVars.containsKey(getName()))
		{
			if (asNewObject)
				return new AtomVariable (freshenVars.get(getName()));
			else
			{
				setName(freshenVars.get(getName()));
				return this;
			}
		}
		else
		{
			String newVarName = getName() + (_nextFreshId++);
			setExistential(true);
			freshenVars.put (getName(), newVarName);
			if (asNewObject)
			{
				AtomVariable var = new AtomVariable (newVarName);
				return var;
			}
			else
			{
				_name = newVarName;
				return this;
			}
		}
		
	}

	/**
	 * Check if this value could be replaced by another value during composition process
	 * Basically a variable can be substituted with anything: variable, constant, skolem... 
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public boolean couldBeSubstitutedWith (AtomArgument val)
	{
		/*		 
		boolean res = false;
		res = (val instanceof ScMappingAtomValVariable);
		return res;
		*/
		return true;
	}		

	/**
	 * If the atom value contains sub atoms values (eg Skolem), substitute
	 * all occurences of <code>oldVal</code> with <code>newVal</code>
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */
	public void substitute (AtomArgument oldVal, AtomArgument newVal)
	{
		
	}
	
	public synchronized void renameVariable (String extension)
	{
		_name = _name + extension;
	}
	
	/*
	 * If this variable is existentially quantified in the head of a mapping,
	 * this field points to the "Skolem Atom" we are creating for it
	 */
	public synchronized boolean isSkolem(){
		return (_skolemDef != null);
	}

	
	
	public synchronized Atom skolemDef(){
		return _skolemDef;
	}

	public synchronized void setSkolemDef(Atom skolemDef){
		_skolemDef = skolemDef;
	}

	public synchronized boolean isExistential(){
		return _existential;
	}
	
	public synchronized void setExistential(boolean ex){
		_existential = ex;
	}

	@Override
	public Element serialize(Document doc) {
		Element result = super.serialize(doc);
		result.setAttribute("type", "variable");
		result.setAttribute("name", _name);
		result.setAttribute("existential", Boolean.toString(_existential));
		if (_skolemDef != null) {
			Element atom = _skolemDef.serializeVerbose(doc);
			atom.setAttribute("type", "skolemDef");
			result.appendChild(atom);
		}
		return result;
	}	

	/**
	 * Returns the {@code AtomVariable} represented by {@code argElement}.
	 * 
	 * @param argElement an {@code Element} produced by {@code serialize(Document)}
	 * @param system 
	 * @return the {@code AtomVariable} represented by {@code argElement}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 */
	public static AtomVariable deserialize(Element argElement, OrchestraSystem system) throws XMLParseException, UnsupportedTypeException {
		String name = argElement.getAttribute("name");
		AtomVariable a = new AtomVariable(name);

		boolean existential = Boolean.parseBoolean(argElement.getAttribute("existential"));
		a.setExistential(existential);

		Element skolemDef = DomUtils.getChildElementByName(argElement, "atom");
		if (skolemDef != null) {
			Atom sd = Atom.deserializeVerbose(skolemDef, new Holder<Integer>(0), system);
			a.setSkolemDef(sd);
		}

		setDeserializeType(argElement, a);
		return a;
	}
	
}
