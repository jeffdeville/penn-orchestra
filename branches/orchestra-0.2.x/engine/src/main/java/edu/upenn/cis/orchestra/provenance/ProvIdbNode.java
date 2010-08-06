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

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;

public class ProvIdbNode extends ProvenanceNode {
	String _label;
	Integer _labelValue = 1;
	boolean _isMapping;

	public ProvIdbNode(String label, String semiringName, boolean isMapping) {
		super(semiringName);
		_label = label;
		_isMapping = isMapping;
	}

	public ProvIdbNode(String label, List<ProvenanceNode> children, String semiringName, boolean isMapping) {
		super(children, semiringName);
		_label = label;
		_isMapping = isMapping;
	}

	public String getLabel() {
		return _label;
	}

	public String toString() {
		StringBuffer str = new StringBuffer(_label + "(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				str.append("*");
			str.append(p.toString());
			first = false;
		}
		str.append(")");

		return new String(str);
	}

	public ProvIdbNode copySelf() {
		return new ProvIdbNode(_label, _semiringName, _isMapping);
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		if(_isMapping)
			results.add(_label + "(");
		else
			results.add("(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add("*");
			results.addAll(p.getStringExpr());
			first = false;
		}
		results.add(")");

		simplify(results);

		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();

		//		results.add(_labelValue + "" + multiOp + "(");
		results.add("(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add(multiOp(_semiringName));
			results.addAll(p.getValueExpr(rule));
			first = false;
		}
		results.add(")");
		if(first){
			Debug.println(getLabel() + " IDB NODE WITH NO CHILDREN?!");
		}
		//		simplify(results);

		return results;
	}

}
