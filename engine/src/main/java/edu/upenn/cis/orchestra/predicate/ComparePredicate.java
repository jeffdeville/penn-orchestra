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
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class ComparePredicate implements Predicate {
	private static final long serialVersionUID = 1L;
	private interface CompareResult {
		boolean eval(int compareResult);
	}
	public enum Op {
		EQ("=", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult == 0;
			}
		}),
		GE(">=", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult >= 0;
			}
		}),
		GT(">", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult > 0;
			}
		}),
		LE("<=", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult <= 0;
			}
		}),
		LT("<", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult < 0;
			}
		}),
		NE("!=","<>", new CompareResult() {
			public boolean eval(int compareResult) {
				return compareResult != 0;
			}
		});

		Op(String op, CompareResult cr) {
			this.op = op;
			this.sqlOp = op;
			this.cr = cr;
		}

		Op(String op, String sqlOp, CompareResult cr) {
			this.op = op;
			this.sqlOp = sqlOp;
			this.cr = cr;
		}

		private final String op;
		private final String sqlOp;
		private final CompareResult cr;

		String getOp() {
			return op;
		}

		String getSqlOp() {
			return sqlOp;
		}

		public boolean eval(int compareResult) {
			return cr.eval(compareResult);
		}
	}

	final int lIndex;
	final int rIndex;
	final Object r;
	final Op op;
	
	ComparePredicate(AbstractRelation ts, String l, Op op, String r) throws NameNotFound, PredicateMismatch {
		if (l == null || r == null) {
			throw new NullPointerException();
		}
		Integer li;
		Integer ri;
		if ((li = ts.getColNum(l)) == null)
			throw new NameNotFound(l,ts);
		if ((ri = ts.getColNum(r)) == null)
			throw new NameNotFound(r,ts);
		// Check that types are the same

		Type t1 = ts.getColType(li);
		Type t2 = ts.getColType(ri);

		if (t1.getClass() != t2.getClass()) {
			throw new PredicateMismatch(t1, t2);
		}

		lIndex = li;
		rIndex = ri;
		this.r = null;
		this.op = op;

	}
	ComparePredicate(AbstractRelation ts, int l, Op op, int r) throws PredicateMismatch {
		lIndex = l;
		rIndex = r;
		this.r = null;

		Type t1 = ts.getColType(l);
		Type t2 = ts.getColType(r);

		if (t1.getClass() != t2.getClass()) {
			throw new PredicateMismatch(t1, t2);
		}
		this.op = op;
	}
	
	ComparePredicate(String l, Op op, Object r, AbstractRelation ts) throws NameNotFound, PredicateLitMismatch {
		if (l == null || r == null) {
			throw new NullPointerException();
		}
		this.op = op;
		Integer li;
		if ((li = ts.getColNum(l)) == null)
			throw new NameNotFound(l,ts);

		lIndex = li;

		Type t = ts.getColType(lIndex);
		if (! t.isValidForColumn(r)) {
			throw new PredicateLitMismatch(t,r);
		}

		this.r = r;
		rIndex = -1;
	}

	ComparePredicate(int lCol, Op op, Object r, AbstractRelation ts) throws PredicateLitMismatch {
		if (r == null) {
			throw new NullPointerException();
		}
		this.op = op;
		lIndex = lCol;

		Type t = ts.getColType(lCol);
		if (! t.isValidForColumn(r)) {
			throw new PredicateLitMismatch(t,r);
		}

		this.r = r;	
		rIndex = -1;
	}

	/**
	 * Create a new comparison predicate that compares two columns in the same tuple
	 * 
	 * @param ts			The tuple's schema
	 * @param lCol			The name of the first column, which will be on the left
	 * 						side of the predicate
	 * @param op			The comparison operator
	 * @param rCol			The name of the second column, which will be on the right
	 * 						side of the predicate
	 * @return				The predicate
	 * @throws NameNotFound
	 * @throws PredicateMismatch
	 */
	public static ComparePredicate createTwoCols(AbstractRelation ts, String lCol, Op op, String rCol) throws NameNotFound, PredicateMismatch {
		return new ComparePredicate(ts, lCol, op, rCol);
	}
	
	/**
	 * Create a new comparison predicate that compares two columns in the same tuple
	 * 
	 * @param ts			The tuple's schema
	 * @param lCol			The index of the first column, which will be on the left
	 * 						side of the predicate
	 * @param op			The comparison operator
	 * @param rCol			The index of the second column, which will be on the right
	 * 						side of the predicate
	 * @return				The predicate
	 * @throws PredicateMismatch
	 */
	public static ComparePredicate createTwoCols(AbstractRelation ts, int lCol, Op op, int rCol) throws PredicateMismatch {
		return new ComparePredicate(ts, lCol, op, rCol);
	}
	
	/**
	 * Create a new comparison predicate that compares a column in a tuple with
	 * a supplied constant
	 * 
	 * @param ts		The tuple's schema
	 * @param lCol		The name of the column, which will be on the left side
	 * 					of the predicate
	 * @param op		The comparison operator
	 * @param r			The constant, which will be on the right side of the predicate
	 * @return			The predicate
	 * @throws NameNotFound
	 * @throws PredicateLitMismatch
	 */
	public static ComparePredicate createColLit(AbstractRelation ts, String lCol, Op op, Object r) throws NameNotFound, PredicateLitMismatch {
		return new ComparePredicate(lCol, op, r, ts);
	}

	/**
	 * Create a new comparison predicate that compares a column in a tuple with
	 * a supplied constant
	 * 
	 * @param ts		The tuple's schema
	 * @param lCol		The index of the column, which will be on the left side
	 * 					of the predicate
	 * @param op		The comparison operator
	 * @param r			The constant, which will be on the right side of the predicate
	 * @return			The predicate
	 * @throws PredicateLitMismatch
	 */
	public static ComparePredicate createColLit(AbstractRelation ts, int lCol, Op op, Object r) throws PredicateLitMismatch {
		return new ComparePredicate(lCol, op, r, ts);
	}
	
	public final String toString() {
		return "(c" + lIndex + " " + op.getOp() + " " + (r == null ? ("c" + rIndex) : r) + ")"; 
	}
	public final String getSqlCondition(AbstractRelation ts, String prefix, String suffix) {
		return prefix + ts.getColName(lIndex) + suffix + " " + op.getSqlOp() + " "
		+ (r == null ? (prefix + ts.getColName(rIndex) + suffix) : ts.getColType(lIndex).getSQLLit(r));
	}

	final public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		ComparePredicate cp = (ComparePredicate) o;
		return (cp.lIndex == lIndex && cp.rIndex == rIndex && (cp.r == null ? r == null : cp.r.equals(r)));
	}

	final public void serialize(Document d, Element e, AbstractRelation ts) {
		e.setAttribute("lField", ts.getColName(lIndex));
		if (r == null) {
			e.setAttribute("rField", ts.getColName(rIndex));
		} else {
			try {
				e.setAttribute("rLiteral", ts.getColType(lIndex).getStringRep(r));
			} catch (ValueMismatchException vm) {
				throw new RuntimeException(vm);
			}
		}
		e.setAttribute("op", op.name());
	}

	static ComparePredicate deserialize(Element el, AbstractRelation ts) throws XMLParseException {
		if (! el.hasAttribute("op")) {
			throw new XMLParseException("Compare predicate must have 'op' attribute", el);
		}
		String opName = el.getAttribute("op");
		Op op;
		try {
			op = Enum.valueOf(Op.class, opName);
		} catch (IllegalArgumentException iae) {
			throw new XMLParseException("Unknown value for 'op' attribute in compare predicate", el);
		}
		if (! el.hasAttribute("lField")) {
			throw new XMLParseException("Compare predicate must have 'lField' attribute", el);
		}
		String lField = el.getAttribute("lField");
		String rField = null;
		Object rLiteral = null;
		if (el.hasAttribute("rField")) {
			rField = el.getAttribute("rField");
		} else if (el.hasAttribute("rLiteral")) {
			rLiteral = ts.getColType(lField).fromStringRep(el.getAttribute("rLiteral"));
		} else {
			throw new XMLParseException("Compare predicate must have either 'rField' or 'rLiteral' attribute", el);
		}
		try {
			if (rField == null) {
				// Compare with literal
				return new ComparePredicate(lField, op, rLiteral, ts);
			} else {
				// Comparison between two fields
				return new ComparePredicate(ts, lField, op, rField);
			}
		} catch (PredicateLitMismatch plm) {
			throw new XMLParseException(plm,el);
		} catch (NameNotFound e) {
			throw new XMLParseException(e,el);
		} catch (PredicateMismatch e) {
			throw new XMLParseException(e,el);
		}
	}
	
	public boolean eval(AbstractTuple<?> t) throws CompareMismatch {
		Integer compareResult;
		Type type = t.getSchema().getColType(lIndex);
		if (r == null) {
			compareResult = type.compare(t, lIndex, rIndex);
		} else {
			compareResult = type.compare(t, lIndex, r);
		}

		if (compareResult == null) {
			return false;
		} else {
			return op.eval(compareResult);
		}
	}
	public Set<Integer> getColumns() {
		if (rIndex == -1) {
			return Collections.singleton(lIndex);
		} else {
			HashSet<Integer> retval = new HashSet<Integer>(2);
			retval.add(lIndex);
			retval.add(rIndex);
			return retval;
		}
	}
}
