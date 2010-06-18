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

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;

public class Byteification {
	private static final byte AND = 101, OR = 102, NOT = 103;

	/**
	 * Compute the serialized representation of a predicate.
	 * 
	 * @param ts		The RelationSchema of the table to which the predicate refers
	 * @param p		The predicate to be serialized
	 * @return		The byte representation, which can be passed to
	 * 					<code>getPredicateFromBytes</code>
	 */
	public static byte[] getPredicateBytes(AbstractRelation ts, Predicate p) {
		if (p == null) {
			return null;
		}
		ByteBufferWriter bbw = new ByteBufferWriter();

		if (p instanceof AndPred) {
			AndPred ap = (AndPred) p;
			bbw.addToBuffer(AND);
			bbw.addToBuffer(getPredicateBytes(ts, ap.p1));
			bbw.addToBuffer(getPredicateBytes(ts, ap.p2));
		} else if (p instanceof OrPred) {
			OrPred op = (OrPred) p;
			bbw.addToBuffer(OR);
			bbw.addToBuffer(getPredicateBytes(ts, op.p1));
			bbw.addToBuffer(getPredicateBytes(ts, op.p2));
		} else if (p instanceof NotPred) {
			NotPred np = (NotPred) p;
			bbw.addToBuffer(NOT);
			bbw.addToBuffer(getPredicateBytes(ts, np.p));
		} else if (p instanceof ComparePredicate) {
			ComparePredicate cp = (ComparePredicate) p;
			bbw.addToBuffer((byte) cp.op.ordinal());
			if (cp.r == null) {
				bbw.addToBuffer(true);
				bbw.addToBuffer(cp.lIndex);
				bbw.addToBuffer(cp.rIndex);
			} else {
				bbw.addToBuffer(false);
				bbw.addToBuffer(cp.lIndex);
				byte[] data = ts.getColType(cp.lIndex).getBytes(cp.r);
				bbw.addToBuffer(data);
			}
		} else if (p == null) {
			throw new NullPointerException();
		} else {
			throw new RuntimeException("Need to implement byteification for predicate class " + p.getClass().getName());
		}

		return bbw.getByteArray();
	}

	public static Predicate getPredicateFromBytes(AbstractRelation ts, byte[] bytes)
	throws PredicateMismatch, PredicateLitMismatch {
		if (bytes == null) {
			return null;
		}
		return getPredicateHelper(ts, new ByteBufferReader(null, bytes));
	}

	public static Predicate getPredicateFromBytes(AbstractRelation ts, byte[] bytes,
			int offset, int length)
	throws PredicateMismatch, PredicateLitMismatch {
		if (bytes == null) {
			return null;
		}
		return getPredicateHelper(ts, new ByteBufferReader(null, bytes, offset, length));
	}

	private static Predicate getPredicateHelper(AbstractRelation ts, ByteBufferReader bytes)
	throws PredicateMismatch, PredicateLitMismatch {
		Predicate retval;
		byte key = bytes.readByte();
		if (key == AND) {
			Predicate p1 = getPredicateHelper(ts, bytes.getSubReader());
			Predicate p2 = getPredicateHelper(ts, bytes.getSubReader());
			retval = new AndPred(p1,p2);
		} else if (key == OR) {
			Predicate p1 = getPredicateHelper(ts, bytes.getSubReader());
			Predicate p2 = getPredicateHelper(ts, bytes.getSubReader());
			retval = new OrPred(p1,p2);
		} else if (key == NOT) {
			Predicate p = getPredicateHelper(ts, bytes.getSubReader());
			retval = new NotPred(p);
		} else {
			Op op;
			try {
				op = Op.values()[key];
			} catch (ArrayIndexOutOfBoundsException e) {
				throw new RuntimeException("Couldn't find comparison predicate for key " + key, e);
			}
			boolean notLit = bytes.readBoolean();
			if (notLit) {
				// Predicate contains two columns
				int colL = bytes.readInt();
				int colR = bytes.readInt();
				retval = new ComparePredicate(ts, colL, op, colR);
			} else {
				int colL = bytes.readInt();
				byte[] literal = bytes.readByteArray();
				Object r = ts.getColType(colL).fromBytes(literal, 0, literal.length);
				retval = new ComparePredicate(colL, op, r, ts);
			}
		}

		if (! bytes.hasFinished()) {
			throw new RuntimeException("Unfinished byte buffer in predicate decoding");
		}

		return retval;
	}
}
