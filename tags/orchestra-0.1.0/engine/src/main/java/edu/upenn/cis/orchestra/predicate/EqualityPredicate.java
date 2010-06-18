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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class EqualityPredicate implements Predicate {
	private static final long serialVersionUID = 1L;

	private final int index1;
	private final int index2;
	private final Object o2;
	private final byte[] b2;
	private final boolean negate;
	
	public EqualityPredicate(AbstractRelation ts, int index1, int index2, boolean negate) throws PredicateMismatch {
		this.index1 = index1;
		this.index2 = index2;

		Type t1 = ts.getColType(index1);
		Type t2 = ts.getColType(index2);

		if (t1.getClass() != t2.getClass()) {
			throw new PredicateMismatch(t1, t2);
		}
		this.negate = negate;
		o2 = null;
		b2 = null;
	}

	public EqualityPredicate(int index, Object o, AbstractRelation ts, boolean negate) throws PredicateLitMismatch {
		if (o == null) {
			throw new NullPointerException();
		}
		this.negate = negate;
		this.index1 = index;
		index2 = -1;

		Type t = ts.getColType(index1);
		if (! t.isValidForColumn(o)) {
			throw new PredicateLitMismatch(t,o);
		}

		this.o2 = o;
		this.b2 = ts.getColType(index).getBytes(o);
	}

	
	@Override
	public boolean eval(AbstractTuple<?> t) throws CompareMismatch {
		if (index2 < 0) {
			return t.equals(index1, b2, o2) ^ negate;
		} else {
			return t.equals(index1, index2) ^ negate;
		}
	}

	@Override
	public String getSqlCondition(AbstractRelation ts, String prefix,
			String suffix) {
		return prefix + ts.getColName(index1) + suffix + (negate ? " <> " : " = ")
		+ (o2 == null ? (prefix + ts.getColName(index2) + suffix) : ts.getColType(index1).getSQLLit(o2));
	}

	@Override
	public void serialize(Document d, Element el, AbstractRelation ts) {
		el.setAttribute("field1", ts.getColName(index1));
		if (o2 == null) {
			el.setAttribute("field2", ts.getColName(index2));
		} else {
			try {
				el.setAttribute("literal", ts.getColType(index1).getStringRep(o2));
			} catch (ValueMismatchException vm) {
				throw new RuntimeException(vm);
			}
		}
		el.setAttribute("negate", Boolean.toString(negate));
	}

	static EqualityPredicate deserialize(Element el, AbstractRelation ts) throws XMLParseException {
		boolean negate = false;
		if (el.hasAttribute("negate")) {
			negate = Boolean.parseBoolean(el.getAttribute("negate"));
		}
		
		if (! el.hasAttribute("field1")) {
			throw new XMLParseException("Compare predicate must have 'field1' attribute", el);
		}
		String field1 = el.getAttribute("field1");
		String field2 = null;
		Object literal = null;
		if (el.hasAttribute("field2")) {
			field2 = el.getAttribute("field2");
		} else if (el.hasAttribute("literal")) {
			literal = ts.getColType(field1).fromStringRep(el.getAttribute("literal"));
		} else {
			throw new XMLParseException("Compare predicate must have either 'field2' or 'literal' attribute", el);
		}
		Integer index1 = ts.getColNum(field1);
		if (index1 == null) {
			throw new XMLParseException("Relation " + ts.getName() + " has no field named " + field1, el);
		}
		
		try {
			if (field2 == null) {
				// Compare with literal
				return new EqualityPredicate(index1, literal, ts, negate);
			} else {
				// Comparison between two fields
				Integer index2 = ts.getColNum(field2);
				if (index2 == null) {
					throw new XMLParseException("Relation " + ts.getName() + " has no field named " + field2, el);
				}
				return new EqualityPredicate(ts, index1, index2, negate);
			}
		} catch (PredicateLitMismatch plm) {
			throw new XMLParseException(plm,el);
		} catch (PredicateMismatch e) {
			throw new XMLParseException(e,el);
		}
	}

	
	@Override
	public Set<Integer> getColumns() {
		if (index2 < 0) {
			return Collections.singleton(index1);
		} else {
			Set<Integer> retval = new HashSet<Integer>();
			retval.add(index1);
			retval.add(index2);
			return retval;
		}
	}

	final public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		EqualityPredicate ep = (EqualityPredicate) o;
		
		return index1 == ep.index1 && index2 == ep.index2 && (o2 == null ? ep.o2 == null : o2.equals(ep.o2));
	}
}
