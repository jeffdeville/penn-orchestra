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

public class ProvUnionNode extends ProvenanceNode {

	public ProvUnionNode(String semiringName){
		super(semiringName);
	}

	public String toString() {
		StringBuffer str = new StringBuffer();

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				str.append("+");
			str.append(p.toString());
			first = false;
		}

		return new String(str);
	}

	public ProvUnionNode copySelf() {
		return new ProvUnionNode(_semiringName);
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add("+");
			results.addAll(p.getStringExpr());
			first = false;
		}

		simplify(results);

		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();
		String plusOp;

		//		Max needs Polish notation and   
		if(RANK_SEMIRING.equalsIgnoreCase(_semiringName) || TROPICAL_MAX_SEMIRING.equalsIgnoreCase(_semiringName) || TROPICAL_MIN_SEMIRING.equalsIgnoreCase(_semiringName)){
			if(TROPICAL_MAX_SEMIRING.equalsIgnoreCase(_semiringName))
				plusOp = "MAX";
			else
				plusOp = "MIN";

			//		results.add(_labelValue + "" + multiOp + "(");
			if(getChildren().size() > 1)
				results.add(plusOp);
			results.add("(");

			boolean first = true;
			for (ProvenanceNode p : getChildren()) {
				if (!first)
					results.add(", ");
				results.addAll(p.getValueExpr(rule));
				first = false;
			}
			results.add(")");
			if(first){
				Debug.println("NO CHILDREN?!");
			}

		}else{
			if(TRUST_SEMIRING.equalsIgnoreCase(_semiringName) || BAG_SEMIRING.equalsIgnoreCase(_semiringName))
				plusOp = "+";
			else
				plusOp = "+";

			boolean first = true;
			for (ProvenanceNode p : getChildren()) {
				if (!first)
					results.add(plusOp);
				results.addAll(p.getValueExpr(rule));
				first = false;
			}
		}		
		
		simplify(results);

		return results;
	}

}
