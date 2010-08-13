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

import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Mapping definition : Mapping atom value

 * @author Olivier Biton
 *****************************************************************
 */
public abstract class AtomArgument 
{
	
	private Type _type=null;

	protected AtomArgument ()
	{
	}
	
	protected AtomArgument (AtomArgument atomVal)
	{
		_type = atomVal._type;
	}
	
	/**
	 * Get a String description of the atom value, conforms to the 
     * flat file format defined in <code>RepositoryDAO</code>
	 * @return Description
	 */
	public abstract String toString ();
	
	public boolean equals (Object obj)
	{
		if (obj instanceof AtomArgument)
			return equals ((AtomArgument) obj);
		else
			return false;
	}	
	
	/**
	 * True if the atom value has the same type, and is equivalent
	 * It will check for attributes equality but won't check on inheritance. Thus if the
	 * parameter extends the class it will be considered equals when the known attributes are 
	 * equal.
	 * It you want to avoid that when calling this method, call it on both elements (x.equals(y) && y.equals(x))
	 * @param atomVal Atom value to compare with
	 * @return True if equivalent
	 */
	public boolean equals (AtomArgument atomVal)
	{
		if (getType()!=null && atomVal.getType()!=null)
			//TODO: Stronger check? Pb is if we check for type equality it requires 
			// exact same type (same capacity...)
			return (getType().getClass().equals(atomVal.getType().getClass()));
		else
			return (getType()==null || atomVal.getType() == null);
		
	}
	
	public int hashCode() {
		return getType().getClass().hashCode();
	}
	
	/**
	 * Get a deep copy of the atom value
	 * @return Deep copy
	 */
	public abstract AtomArgument deepCopy (); 

	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public abstract AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject);
	
	
	/**
	 * Check if this value could be replaced by another value during composition process
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public abstract boolean couldBeSubstitutedWith (AtomArgument val);

	
	/**
	 * If the atom value contains sub atoms values (eg Skolem), substitute
	 * all occurences of <code>oldVal</code> with <code>newVal</code>
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */
	public abstract void substitute (AtomArgument oldVal, AtomArgument newVal);
	
	
	public abstract void renameVariable (String extension);
	
	public abstract void renameVariable (Map<String,String> renameTable);
	
	public void setType (Type type){
		_type = type;
	}
	
	public Type getType ()
	{
		return _type;
	}
	
	/**
	 * Returns an XML representation of this {@code AtomArgument}.
	 * 
	 * @param doc only used to create {@code Elements}.
	 * @return an XML representation of this {@code AtomArgument}
	 */
	public Element serialize(Document doc) {
		Element arg = doc.createElement("atomArgument");
		if (_type != null) {
			Element argType = doc.createElement("dataType");
			_type.serialize(doc, argType);
			arg.appendChild(argType);
		}
		return arg;
	}
	
	/**
	 * Sets {@code arg}'s {@code OptimizerType}, if any, to that of the {@code
	 * AtomArgument} represented by {@code argElement}.
	 * 
	 * @param argElement an {@code AtomArgument} produced by {@code
	 *            serialize(Document)}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 */
	protected static void setDeserializeType(Element argElement,
			AtomArgument arg) throws XMLParseException,
			UnsupportedTypeException {
		Element typeElement = DomUtils.getChildElementByName(argElement,
				"dataType");
		Type type = null;
		if (typeElement != null) {
			type = Type.deserialize(typeElement);
			arg.setType(type);
		}
	}

	/**
	 * Returns the {@code AtomArgument} represented by {@code element}.
	 * 
	 * @param element an {@code Element} produced by {@code serialize(Document)}
	 * @return the {@code AtomArgument} represented by {@code element}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 */
	public static AtomArgument deserialize(Element element) throws XMLParseException, UnsupportedTypeException {
		String argType = element.getAttribute("type");
		AtomArgument result = null;
		if ("const".equals(argType)){
			result = AtomConst.deserialize(element);
		} else if ("skolem".equals(argType)){
			result = AtomSkolem.deserialize(element);
		} else if ("variable".equals(argType)){
			result = AtomVariable.deserialize(element);
		} else {
			throw new XMLParseException("Unknown AtomArgument type: [" + argType + "].");
		}
		return result;
	}
}


