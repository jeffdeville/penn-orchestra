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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/****************************************************************
 * Mapping definition : Mapping atom values // Skolem
 * Note: Vars do not have to reference variables existing in the mapping atoms. The
 * <code>equals</code> method will be used.

 * @author Olivier Biton
 *****************************************************************
 */
public class AtomSkolem extends AtomArgument {

	/**
	 * Name of the skolem function 
	 */
	private String _name;
	
	//TODO:MID Check if skolem function variables/skolems exist in mapping
	/**
	 * Skolem function parameters, must appear in the mapping
	 */
	private List<AtomArgument> _params = new Vector<AtomArgument> ();
	
	
	/**
	 * Creates a new skolem atom value
	 * @param name Name of the skolem function
	 * @param vars List of skolem parameters
	 */
	public AtomSkolem (String name, List<AtomArgument> vars)
	{
		_name = name;
		_params.addAll(vars);
	}
	
	/**
	 * Creates a deep copy of the skolem function. <BR>
	 * To benefit from polymorphism, use <code>deepCopy()</code>
	 * @param skolem Skolem to deep copy
	 */
	protected AtomSkolem (AtomSkolem skolem)
	{
		super (skolem);
		_name = skolem.getName();
		for (AtomArgument var : skolem.getParams())
			_params.add(var.deepCopy());
	}
	
	/**
	 * Get the function name
	 * @return Function name
	 */
	public String getName() {
		return _name;
	}

	/**
	 * Get the function variables
	 * @return List of variables
	 */
	public synchronized List<AtomArgument> getParams() {
		return _params;
	}
	
	
	/**
	 * Creates a deep copy of the skolem atom value
	 * @return deep copy
	 * @see AtomSkolem#deepCopy()
	 */
	public synchronized AtomArgument deepCopy() {
		return new AtomSkolem (this);
	}
	
	
	/**
	 * Test equality
	 * @param atomVal Atom value to compare with
	 * @return True it <code>atomVal</code> is a skolem, has the same name and the same variables
	 */
	public synchronized boolean equals(AtomArgument atomVal) {
		if (atomVal instanceof AtomSkolem)
		{
			AtomSkolem skolem2 = (AtomSkolem) atomVal;
			boolean equals = getName().equals(skolem2.getName());
			equals = equals && super.equals(atomVal);
			if (equals && getParams().size() == skolem2.getParams().size())
			{
				Iterator<AtomArgument> itV1 = getParams().iterator();
				Iterator<AtomArgument> itV2 = skolem2.getParams().iterator();
				while (equals && itV1.hasNext() && itV2.hasNext())
					equals = itV1.next().equals(itV2.next());
			}
			else 
				equals = false;
			return equals;
		}
		else
			return false;
	}
	

	public int hashCode() {
		return _name.hashCode();
	}

   /**
    * Returns a description of the skolem, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Mapping description
    */
	public synchronized String toString() {
		StringBuffer buff = new StringBuffer ();
		buff.append(getName());
		buff.append("(");
		boolean firstParam = true;
		for (AtomArgument param : getParams())
		{
			buff.append((firstParam?"":",") + param.toString());
			firstParam = false;
		}
		buff.append (")");
		return buff.toString();
	}


	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public synchronized AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		List<AtomArgument> params = new ArrayList<AtomArgument> ();
		for (AtomArgument param : getParams())
			params.add(param.fresh(freshenVars, asNewObject));
		if (asNewObject)
		{
			AtomSkolem newVal = new AtomSkolem (getName(), params);
			newVal.setType(getType());
			return newVal;
		}
		else
		{
			_params = params;
			return this;
		}
	}
	

	
	/**
	 * Check if this value could be replaced by another value during composition process
	 * Basically a skolem can only be substituted with a skolem having the same name and a 
	 * set of variables with the same size. (Would imply that in composition these 
	 * variables are equal one to one). 
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public synchronized boolean couldBeSubstitutedWith (AtomArgument val)
	{
		boolean res = false;
		if (val instanceof AtomSkolem)
		{
			AtomSkolem otherSk =  (AtomSkolem) val;
			res = otherSk.getName().equals(getName());
			res = res && otherSk.getParams().size()==getParams().size();
			Iterator<AtomArgument> itParams1 = getParams().iterator();
			Iterator<AtomArgument> itParams2 = otherSk.getParams().iterator();
			while (res && itParams1.hasNext() && itParams2.hasNext())
			{
				AtomArgument val1 = itParams1.next();
				AtomArgument val2 = itParams2.next();
				res = val1.couldBeSubstitutedWith(val2) || val2.couldBeSubstitutedWith(val1);
			}
				 
		}
		return res;
	}	
	
	/**
	 * If the atom value contains sub atoms values (eg Skolem), substitute
	 * all occurences of <code>oldVal</code> with <code>newVal</code>
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */
	public synchronized void substitute (AtomArgument oldVal, AtomArgument newVal)
	{
		for (int i = 0 ; i < _params.size() ; i++)
		{
			AtomArgument param = _params.get(i);
			if (param.equals(oldVal) && oldVal.equals(param))
			{
				_params.remove(i);
				_params.add(i, newVal);
			}
		}
	}
	
	public synchronized void renameVariable (String extension)
	{
		for (AtomArgument val : getParams())
			val.renameVariable(extension);
	}
	
	public synchronized void renameVariable (Map<String,String> renameTable)
	{
		for (AtomArgument val : getParams())
			val.renameVariable(renameTable);
	}
	
	@Override
	public Element serialize(Document doc) {
		Element result = super.serialize(doc);
		result.setAttribute("type", "skolem");
		result.setAttribute("name", _name);
		//It looks like _vaule is always a String?
		Element parameters = DomUtils.addChild(doc, result, "parameters");
		for (AtomArgument param : _params) {
			parameters.appendChild(param.serialize(doc));
		}
		return result;
	}	

	/**
	 * Returns the {@code AtomSkolem} represented by {@code argElement}.
	 * 
	 * @param argElement an {@code Element} produced by {@code serialize(Document)}
	 * @return the {@code AtomSkolem} represented by {@code argElement}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 */
	public static AtomSkolem deserialize(Element argElement) throws XMLParseException, UnsupportedTypeException {
		String name = argElement.getAttribute("name");
		Element parametersElement = DomUtils.getChildElementByName(argElement, "parameters");
		List<Element> parametersList = DomUtils.getChildElements(parametersElement);
		List<AtomArgument> parameters = OrchestraUtil.newArrayList();
		for (Element parameterElement : parametersList) {
			String argType = parameterElement.getAttribute("type");
			if ("const".equals(argType)){
				parameters.add(AtomConst.deserialize(parameterElement));
			} else if ("skolem".equals(argType)){
				parameters.add(AtomSkolem.deserialize(parameterElement));
			} else if ("variable".equals(argType)){
				parameters.add(AtomVariable.deserialize(parameterElement));
			} else {
				throw new XMLParseException("Unknown AtomArgument type: [" + argType + "].");
			}
		}
		AtomSkolem a = new AtomSkolem(name, parameters);
		setDeserializeType(argElement, a);
		return a;
	}
}
