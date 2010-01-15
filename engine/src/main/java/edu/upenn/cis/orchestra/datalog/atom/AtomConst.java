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

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Mapping definition : Mapping atom constant value
 * @author Olivier Biton
 *****************************************************************
 */
public class AtomConst extends AtomArgument {

	/**
	 * Value of the constant atom parameter
	 */
	private Object _value;
	
	/**
	 * Creates a new Mapping atom constant value
	 * @param value Constant value
	 */
	public AtomConst (Object value)
	{
		_value = value;
	}

	/**
	 * Creates a deep copy of the atom constant value. <BR>
	 * To benefit from polymorphism, use the method <code>deepCopy()</code>
	 * @param cst Atom constant parameter to copy
	 * @see AtomConst#deepCopy()
	 */
	protected AtomConst (AtomConst cst)
	{
		super (cst);
		_value = cst.getValue();
	}

	@Override
	public synchronized void setType(Type type) {
		super.setType(type);
		try
		{
			if (getValue() instanceof String && getType().getClassObj() != String.class)
				_value = getType().fromStringRep((String) getValue());
		} catch (XMLParseException ex)
		{
			ex.printStackTrace();
			//TODO... raise value mismatch??
		}
	}

	
	/**
	 * @return True if this atom value is a constant and has the same value
	 */	
	public synchronized boolean equals(AtomArgument atomVal) {
		if (atomVal instanceof AtomConst)
		{
			boolean equals = super.equals(atomVal);
			if (_value==null)
				return (equals && ((AtomConst) atomVal).getValue()==null);
			else
				return (equals && _value.equals(((AtomConst) atomVal).getValue()));
			
		}
		else
			return false;
	}

	public int hashCode() {
		return _value.hashCode();
	}
	
	/** 
	 * @return String representation of the constant value
	 */
	public synchronized String toString ()
	{		
		if (getValue() == null)
			return "-";
		else 
		{
			//TODO remove, just to see if still used somewhere...
			if (getType()==null)				
			{
				Debug.println ("OptimizerType is null");
				return getValue().toString();
			} else
				return getType().getSQLLit(getValue());
		}
	}

	/**
	 * Get the constant value
	 * @return Constant value
	 */
	public synchronized Object getValue() {
		return _value;
	}

	/**
	 * Get a deep copy of this constant value
	 * @return Deep copy
	 * @see AtomConst#ScMappingAtomValConst(AtomConst)
	 */
	public synchronized AtomArgument deepCopy() {
		return new AtomConst(this);
	}
	
	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition. <BR>
	 * For a constant, this is the constant itself (no need to create a new object...)
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		return this;
	}

	
	/**
	 * Check if this value could be replaced by another value during composition process
	 * Basically a constant can only be substituted with a constant having the same value!
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public synchronized boolean couldBeSubstitutedWith (AtomArgument val)
	{
		boolean res = false;
		if (val instanceof AtomConst)
			res = ((AtomConst) val).equals(this);
		return res;
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
	
	public void renameVariable (String extension)
	{}
	
	public boolean isLabeledNull(){
		if (getValue() != null && getValue().toString() != null)
			return getValue().toString().startsWith("NULL(");
		else
			return false;
	}
	
	public String getLabeledNullValue(){
		if(isLabeledNull())
			return getValue().toString().substring(5, getValue().toString().length()-1);
		else
			return null;
	}

	@Override
	public Element serialize(Document doc) {
		Element result = super.serialize(doc);
		result.setAttribute("type", "const");
		//It looks like _vaule is always a String?
		DomUtils.addChildWithText(doc, result, "value", _value == null ? "null"
				: _value.toString());
		return result;
	}	

	/**
	 * Returns the {@code AtomConst} represented by {@code argElement}.
	 * 
	 * @param argElement an {@code Element} produced by {@code serialize(Document)}
	 * @return the {@code AtomConst} represented by {@code argElement}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 */
	public static AtomConst deserialize(Element argElement) throws XMLParseException, UnsupportedTypeException {
		String value = DomUtils.getChildElementByName(argElement, "value").getTextContent();
		AtomConst a = new AtomConst("null".equals(value) ? null : value);
		setDeserializeType(argElement, a);
		return a;
	}

}
