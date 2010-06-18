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

import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;


import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * 
 * @author gkarvoun
 *
 */
public class RuleEqualityAtom
{
	private AtomArgument _val1, _val2;
	
	public RuleEqualityAtom (AtomArgument val1, AtomArgument val2)
				throws CompositionException
	{
		if (val1.couldBeSubstitutedWith(val2))
		{
			_val1 = val1;
			_val2 = val2;
		} else 
		{
			if (val2.couldBeSubstitutedWith(val1))
			{
				_val1 = val2;
				_val2 = val1;
			}
			else
				throw new CompositionException ("Incompatible values: " + val1.toString() + " // " + val2.toString());
		}
		
	}
	
	public AtomArgument getVal1 ()
	{
		return _val1;
	}
	
	public AtomArgument getVal2 ()
	{
		return _val2;
	}
	
	
	public AtomArgument[] getValues ()
	{
		return new AtomArgument[]{getVal1(),getVal2()};
	}
	
	/**
	 * Get a fresh version of this equality atom, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true then a new freshen eq atom will be returned, if false this atom will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public RuleEqualityAtom fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		RuleEqualityAtom res=null;
		AtomArgument val1 =  getVal1().fresh(freshenVars, asNewObject);
		AtomArgument val2 =  getVal2().fresh(freshenVars, asNewObject);
		
		if (asNewObject)
		{
			try
			{
				res = new RuleEqualityAtom(val1, val2);
			} catch (CompositionException ex)
			{
				// Cannot happen during a copy!
				assert false : "CompositionException should not be raised while freshening vars!";
			}
		}
		else
		{
			_val1 = val1;
			_val2 = val2;
			res = this;
		}
		return res;
	}
	
	@Override
	public String toString ()
	{
		StringBuffer buffer = new StringBuffer ();
		buffer.append(getVal1().toString());
		buffer.append ("=");
		buffer.append(getVal2().toString());
		return buffer.toString();
	}
	
	/**
	 * Returns an {@code Element} representing this {@code RuleEqualityAtom}.
	 * 
	 * @param doc
	 * @return an {@code Element} representing this {@code RuleEqualityAtom}
	 */
	public Element serialize(Document doc) {
		Element rea = doc.createElement("ruleEqualityAtom");
		Element value1 = _val1.serialize(doc);
		value1.setAttribute("position", "1");
		rea.appendChild(value1);
		Element value2 = _val2.serialize(doc);
		value2.setAttribute("position", "2");
		rea.appendChild(value2);
		return rea;
	}
	
	/**
	 * Returns the {@code RuleEqualityAtom} represented by {@code element}.
	 * 
	 * @param element an {@code Element} produced by {@code serialize(Document)}
	 * @return the {@code RuleEqualityAtom} represented by {@code element}
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 * @throws CompositionException
	 */
	public static RuleEqualityAtom deserialize(Element element)
			throws XMLParseException, UnsupportedTypeException,
			CompositionException {
		List<Element> values = DomUtils.getChildElementsByName(element, "atomArgument");
		AtomArgument v1 = AtomArgument.deserialize(values.get(0));
		AtomArgument v2 = AtomArgument.deserialize(values.get(1));
		return new RuleEqualityAtom(v1, v2);
	}
}
