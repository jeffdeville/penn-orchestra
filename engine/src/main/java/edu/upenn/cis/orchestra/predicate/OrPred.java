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
package edu.upenn.cis.orchestra.predicate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


/**
 * Class to represent the disjunction of two predicates.
 * 
 * @author Nick Taylor
 */
public class OrPred implements Predicate  {
	private static final long serialVersionUID = 1L;
	Predicate p1;
	Predicate p2;
	/**
	 * Combine two predicates by taking their disjunction
	 * 
	 * @param p1	One of the two predicates to combine
	 * @param p2	The other one
	 */
	public OrPred(Predicate p1, Predicate p2) {
		this.p1 = p1;
		this.p2 = p2;
	}
	public boolean eval(AbstractTuple<?> t) throws CompareMismatch {
		return (p1.eval(t) || p2.eval(t));
	}
	public String toString() {
		return "(" + p1.toString() + " || " + p2.toString() + ")";
	}
	public String getSqlCondition(AbstractRelation ts, String prefix, String suffix) {
		return "(" + p1.getSqlCondition(ts, prefix, suffix) + " OR " + p2.getSqlCondition(ts, prefix, suffix) + ")";
	}
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		OrPred op = (OrPred) o;
		return ((p1.equals(op.p1) && p2.equals(op.p2)) || (p1.equals(op.p2) && p2.equals(op.p1)));
	}
	public void serialize(Document d, Element el, AbstractRelation ts) {
		Element el1 = DomUtils.addChild(d, el, "pred1");
		XMLification.serialize(p1, d, el1, ts);
		Element el2 = DomUtils.addChild(d, el, "pred2");
		XMLification.serialize(p2, d, el2, ts);
	}
	static Predicate deserialize(Element el, AbstractRelation ts) throws XMLParseException {
		List<Element> children = DomUtils.getChildElements(el);
		if (children.size() != 2) {
			throw new XMLParseException("Or predicate must contain exactly two other predicates", el);
		}
		return new OrPred(XMLification.deserialize(children.get(0), ts), XMLification.deserialize(children.get(1), ts));
	}
	public Set<Integer> getColumns() {
		Set<Integer> cols = new HashSet<Integer>();
		cols.addAll(p1.getColumns());
		cols.addAll(p2.getColumns());
		return cols;
	}
}