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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;
import edu.upenn.cis.orchestra.sql.ISqlFromItem;

public abstract class ProvenanceNode {
	private List<ProvenanceNode> _children;
	private ProvenanceNode _parent = null;
	public String _semiringName;
	public static final String TRUST_SEMIRING = "TRUST";
	public static final String BAG_SEMIRING = "MULTIPLICITY";
	public static final String RANK_SEMIRING = "WEIGHT";
	public static final String TROPICAL_MAX_SEMIRING = "MAX";
	public static final String TROPICAL_MIN_SEMIRING = "MIN";

	public ProvenanceNode(String semiringName) {
		_children = new ArrayList<ProvenanceNode>();
		_semiringName = semiringName;
	}

	public ProvenanceNode(Collection<ProvenanceNode> children, String semiringName) {
		_children = new ArrayList<ProvenanceNode>();

		_children.addAll(children);

		_semiringName = semiringName;
	}

	public List<ProvenanceNode> getChildren() {
		return _children;
	}

	public void addChild(ProvenanceNode child) {
		_children.add(child);
		child._parent = this;
	}

	public void prependChild(ProvenanceNode child) {
		_children.add(0, child);
		child._parent = this;
	}

	public ProvenanceNode replaceChild(ProvenanceNode newChild, int index) {
		ProvenanceNode oldChild = _children.remove(index);
		_children.add(0, newChild);
		newChild._parent = this;
		return oldChild;
	}
	
	public ProvenanceNode getChildAt(int i) {
		return _children.get(i);
	}

	public int getNumChildren() {
		return _children.size();
	}

	public ProvenanceNode getParent() {
		return _parent;
	}

	public int size() {
		return getNumChildren();
	}

	public abstract ProvenanceNode copySelf();

	public ProvenanceNode deepCopy() {
		ProvenanceNode n = copySelf();
		for (ProvenanceNode p : getChildren())
			n.addChild(p.deepCopy());

		return n;
	}

	/**
	 * Given the root of an identical subtree, returns the node in
	 * that tree that is identical to the findThis node in our tree
	 * 
	 * @param findThis
	 * @param twinTree
	 * @return
	 */
	public ProvenanceNode getCorresponding(ProvenanceNode findThis,
			ProvenanceNode twinTree) {
		if (this == findThis)
			return twinTree;
		else {
			for (int i = 0; i < getNumChildren(); i++) {
				ProvenanceNode ret = getChildAt(i).getCorresponding(findThis, 
						twinTree.getChildAt(i));

				if (ret != null)
					return ret;
			}
		}
		return null;
	}

	/**
	 * Coalesces consecutive string elements in the list
	 * 
	 * @param list
	 */
	public void simplify(List<Object> list) {
		int i = 0;

		while (i < list.size() - 1) {
			if (list.get(i) instanceof String &&
					list.get(i+1) instanceof String) {
				String str = (String)list.get(i) + (String)list.get(i+1);

				list.set(i, str);
				list.remove(i+1);
			} else
				i++;
		}
	}

	/**
	 * Returns the SQL expression from our provenance expression.
	 * Assumes '' quotes for string, || operator for concatenation.
	 * Assumes that all variables can be concatenated regardless of
	 * datatype.  (Not sure what happens when an int get strcat'ed.)
	 * 
	 * @param map Variable mapping to SQL name
	 * @param typemap Variable mapping to Orchestra datatype
	 * @return
	 */
	public String getSqlExpression(Map<String,String> map, Map<String,Type> typemap,
			String castTo) {
		if(Config.isMYSQL())
			return getMySqlExpression(map, typemap, castTo);

		List<Object> result = getStringExpr();

		StringBuffer ret = new StringBuffer();

		ret.append("(");

		boolean first = true;
		for (Object o : result) {
			if (!first)
				ret.append(" || ");
			if (o instanceof String)
				ret.append("'" + o.toString() + "'");
			else if (o instanceof AtomVariable) {
				if (typemap.get(o.toString()) instanceof StringType)
					ret.append("'\"' || " + map.get(o.toString()) + "|| '\"'");
				else
					ret.append("CAST(" + map.get(o.toString()) + " AS " + castTo + ")");
			} else
				ret.append(o.toString());

			first = false;
		}

		ret.append(")");

		return new String(ret);
	}

	public String getMySqlExpression(Map<String,String> map, Map<String,Type> typemap,
			String castTo){
		List<Object> result = getStringExpr();

		StringBuffer ret = new StringBuffer();
		ret.append("CONCAT(");

		boolean first = true;
		for (Object o : result) {
			if (!first)
				ret.append(" , ");
			if (o instanceof String)
				ret.append("'" + o.toString() + "'");
			else if (o instanceof AtomVariable) {
				if (typemap.get(o.toString()) instanceof StringType)
					ret.append("'\"' , " + map.get(o.toString()) + ", '\"'");
				else
					ret.append("CAST(" + map.get(o.toString()) + " AS " + castTo + ")");
			} else
				ret.append(o.toString());

			first = false;
		}

		ret.append(")");

		return new String(ret);

	}

	public String getSqlValueExpression(Map<String,String> map, Map<String,Type> typemap,
			Rule rule, List<? extends ISqlFromItem> fr) throws InvalidAssignmentException {
		List<Object> result = getValueExpr(rule);

		StringBuffer ret = new StringBuffer();
		ret.append("(");
		int i = 0;

		for (Object o : result) {
			if (o instanceof String){
				if(multiOp(_semiringName).equals(o) || "(".equals(o) || ")".equals(o)){
					ret.append(o.toString());
				}else{
					String s = (String)o;	
					
					String n = s.replace("$$", fr.get(i).getAlias());
					ret.append(n);
					i++;
				}
			}else if (o instanceof AtomVariable) {
				ret.append(map.get(o.toString()));
			} else
				ret.append(o.toString());
		}

		ret.append(")");

		return new String(ret);
	}

	public static String multiOp(String semiringName){
		if(RANK_SEMIRING.equalsIgnoreCase(semiringName) || TROPICAL_MAX_SEMIRING.equalsIgnoreCase(semiringName) || TROPICAL_MIN_SEMIRING.equalsIgnoreCase(semiringName))
			return "+";
		else if(TRUST_SEMIRING.equalsIgnoreCase(semiringName) || BAG_SEMIRING.equalsIgnoreCase(semiringName))
			return "*";
		else
			return "*";
	}

	public abstract List<Object> getStringExpr();

	public abstract List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException;
}
