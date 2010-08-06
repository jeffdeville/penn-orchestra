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
 * Class to represent the negation of a predicate
 * 
 * @author Nick Taylor
 */
public class NotPred implements Predicate  {
	private static final long serialVersionUID = 1L;
	Predicate p;
	/**
	 * Create a new predicate that returns the negation
	 * of another predicate
	 * 
	 * @param p		The predicate to negate
	 */
	public NotPred(Predicate p) {
		this.p = p;
	}
	public boolean eval(AbstractTuple<?> t) throws CompareMismatch {
		return (! p.eval(t));
	}
	public String toString() {
		return "!(" + p.toString() + ")";
	}
	public String getSqlCondition(AbstractRelation ts, String prefix, String suffix) {
		return "NOT " + p.getSqlCondition(ts, prefix, suffix);
	}
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		NotPred np = (NotPred) o;
		return p.equals(np.p);
	}
	public void serialize(Document d, Element el, AbstractRelation ts) {
		Element el1 = DomUtils.addChild(d, el, "pred");
		XMLification.serialize(p, d, el1, ts);
	}
	static Predicate deserialize(Element el, AbstractRelation ts) throws XMLParseException {
		List<Element> children = DomUtils.getChildElements(el);
		if (children.size() != 1) {
			throw new XMLParseException("Not predicate must contain exactly two other predicates", el);
		}
		return new NotPred(XMLification.deserialize(children.get(0), ts));
	}
	public Set<Integer> getColumns() {
		return p.getColumns();
	}

}