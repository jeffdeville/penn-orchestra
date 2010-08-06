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
package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;

public class ProvEdbNode extends ProvenanceNode {
	List<AtomArgument> _varNames;
	String _label;
	String _assgnExpr;

	public ProvEdbNode(String label, List<AtomArgument> varNames, String semiringName, String assgnExpr) {
		super(semiringName);
		_label = label;
		_varNames = new ArrayList<AtomArgument>();

		_varNames.addAll(varNames);
		//		for (ScMappingAtomArgument arg : varNames)
		//			_varNames.add(arg.deepCopy());

		_assgnExpr = assgnExpr;
	}

	public String getLabel() {
		return _label;
	}

	public List<AtomArgument> getVarNames() {
		return _varNames;
	}

	public ProvEdbNode copySelf() {
		return new ProvEdbNode(_label, _varNames, _semiringName, _assgnExpr);
	}

	public String toString() {
		String ret = _label + "(";

		boolean first = true;
		for (AtomArgument a: _varNames) {
			if (!first)
				ret = ret + ",";
			first = false;
			ret = ret + a.toString();
		}

		return ret + ")";
	}

	public String findMatchingAssignment(String assgnExpr){
		String[] assgns = assgnExpr.split("\n");
//		java.util.regex.Pattern p = java.util.regex.Pattern.compile("CASE ([A-Za-z0-9.*()_]*) SET (.*)");
		java.util.regex.Pattern p = java.util.regex.Pattern.compile("CASE ([^ ]*) SET (.*)");

		for(String expr : assgns){
			Matcher m = p.matcher(expr);
			if(m.matches()){
				String inputExpr = m.group(1);
				java.util.regex.Pattern ip = java.util.regex.Pattern.compile(inputExpr);
				Matcher im = ip.matcher(getLabel());

				//			if(getLabel().equals(inputExpr))
				if(im.matches()){
					return m.group(2);
				}
			}
		}
		java.util.regex.Pattern d = java.util.regex.Pattern.compile("DEFAULT SET (.*)");
		Matcher dm = d.matcher(assgnExpr);
		if(dm.find()){
			return dm.group(1);
		}else{
			return defaultValue(_semiringName);
		}
	}
	
	public static String defaultValue(String semiringName) {
		if(TRUST_SEMIRING.equalsIgnoreCase(semiringName) || BAG_SEMIRING.equalsIgnoreCase(semiringName))
			return "0";
		else if(TROPICAL_MAX_SEMIRING.equalsIgnoreCase(semiringName))
			return String.valueOf(Integer.MIN_VALUE);
		else if(RANK_SEMIRING.equalsIgnoreCase(semiringName) || TROPICAL_MIN_SEMIRING.equalsIgnoreCase(semiringName))
			return String.valueOf(Integer.MAX_VALUE);
		else
			return null;
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		results.add(_label + "(");

		//results.addAll(_varNames);
		boolean first = true;
		for (AtomArgument a: _varNames) {
			if (!first)
				results.add(",");
			first = false;
			results.add(a);
		}

		results.add(")");

		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();

//		results.add("{");
		//		results.add(_label + "(");
		//results.addAll(_varNames);
		boolean first = true;
		//		results.add("value");
		boolean found = false;
		for(int i = 0; i < rule.getBody().size(); i++){
			if(!found && _label.equals(rule.getBody().get(i).getRelationContext().toString())){
				found = true;
				//				results.add("R"+i+"." + Relation.valueAttrName);
				//				results.add(_assgnExpr);
				String val = findMatchingAssignment(_assgnExpr);
				if(val != null)
					results.add(val);
				else
					throw new InvalidAssignmentException(getLabel());
			}
		}
//		results.add("}");
		
		if(!found){
			Debug.println("WHAT IS THIS?");
		}
		
		return results;
	}

}
