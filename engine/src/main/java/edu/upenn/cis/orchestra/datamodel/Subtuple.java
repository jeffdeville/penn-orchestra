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
package edu.upenn.cis.orchestra.datamodel;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public class Subtuple {
	private static final int[] hashMultipliers = {17, 37, 89, 157, 293, 601, 1201, 2411, 4801, 9601, 20011};
	private static final int[] hashMultipliers2 = {29, 59, 113, 229, 461, 919, 1847, 3677, 7193, 14407, 29009};

	private final Object[] atts;
	private final Type[] types;
	private final int hashCode;
	private final int hashCode2;
	
	public Subtuple(AbstractTuple<?> t, final int[] indices) {
		this(t, indices.length, new Iterator<Integer>() {
			int next = 0;
			public boolean hasNext() {
				return (next < indices.length);
			}

			public Integer next() {
				if (next >= indices.length) {
					throw new NoSuchElementException();
				} else {
					return indices[next++];
				}
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		});
	}
	
	private Subtuple(AbstractTuple<?> t, int numCols, Iterator<Integer> indices) {
		int hashCode = 0;
		int hashCode2 = 0;
		atts = new Object[numCols];
		types = new Type[numCols];
		int i = 0;
		while (indices.hasNext()) {
			int origPos = indices.next();
			Object o = t.getValueOrLabeledNull(origPos);
			atts[i] = o;
			types[i] = t.getSchema().getColType(origPos);
			int colCode;
			if (o == null) {
				colCode = 0;
			} else if (o instanceof LabeledNull) {
				colCode = ((LabeledNull) o).getLabel();
			} else {
				try {
					colCode = types[i].getHashCode(o);
				} catch (ValueMismatchException e) {
					throw new RuntimeException("OptimizerType error creating subtuple", e);
				}
			}
			if (i < hashMultipliers.length) {
				hashCode += hashMultipliers[i] * colCode;
			} else {
				hashCode = 37 * hashCode + colCode;
			}
			if (i < hashMultipliers2.length) {
				hashCode2 += hashMultipliers[i] * colCode;
			} else {
				hashCode2 = 37 * hashCode2 + colCode;
			}
			++i;
		}
		
		if (i != numCols) {
			throw new IllegalArgumentException("Iterator has different number of elements than numCols indicates it should");
		}
		
		this.hashCode = hashCode;
		this.hashCode2 = hashCode2;
	}
	
	public Subtuple(AbstractTuple<?> t, SortedSet<Integer> indices) {
		this(t,indices.size(),indices.iterator());
	}
	
	public int hashCode() {
		return hashCode;
	}
	
	public int hashCode2() {
		return hashCode2;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		Subtuple s = (Subtuple) o;
		final int numAtts = atts.length;
		
		if (numAtts != s.atts.length) {
			return false;
		}
		
		
		for (int i = 0; i < numAtts; ++i) {
			if (atts[i] == null) {
				if (s.atts[i] != null) {
					return false;
				}
			} else if (s.atts[i] == null) {
				return false;
			} else {
				try {
					if (atts[i] instanceof LabeledNull) {
						if (s.atts[i] instanceof LabeledNull) {
							if (((LabeledNull) atts[i]).getLabel() != ((LabeledNull) s.atts[i]).getLabel()) {
								return false;
							}
						} else {
							return false;
						}
					} else if (s.atts[i] instanceof LabeledNull) {
						return false;
					} else if (types[i].compareTwo(atts[i], s.atts[i]) != 0) {
						return false;
					}
				} catch (CompareMismatch e) {
					throw new RuntimeException("OptimizerType error comparing subtuples", e);
				}
			}
		}
		
		return true;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder("(");
		final int numAtts = atts.length;
		
		for (int i = 0; i < numAtts; ++i) {
			Object o = atts[i];
			if (o == null) {
				sb.append("-");
			} else if (o instanceof String) {
				sb.append("'" + o + "'");
			} else{
				sb.append(o.toString());
			}
			if (i != numAtts -1) {
				sb.append(",");
			}
		}
		sb.append(")");
		return sb.toString();
	}
	
	public Object get(int i) {
		return atts[i];
	}
	
	public Type getType(int i) {
		return types[i];
	}
	
	public byte[] getBytes(int i) {
		return types[i].getBytes(atts[i]);
	}
	
	public int getSize() {
		return atts.length;
	}	
}