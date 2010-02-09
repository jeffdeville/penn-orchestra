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

import java.io.Serializable;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * @author netaylor
 *
 * @param <S>
 */
public abstract class AbstractTuple<S extends AbstractRelation> implements Serializable {
	private static final long serialVersionUID = 1L;

	public static class LabeledNull {
		private final int label;
		public  LabeledNull(int label) {
			this.label = label;
		}
		public int getLabel() {
			return label;
		}
		public int hashCode() {
			return label;
		}
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			LabeledNull ln = (LabeledNull) o;
			return (label == ln.label);
		}
		public String toString() {
			return "NULL(" + label + ")";
		}
	}

	protected final S schema;

	/**
	 * Create a new, empty tuple for the specified schema.
	 * @param schema		The schema of the relation that this tuple conforms to
	 */
	public AbstractTuple(S schema) {
		if (! schema.isFinished()) {
			throw new RuntimeException("Attempt to create tuple from unfinished schema");
		}
		this.schema = schema;
	}

	public S getSchema() {
		return schema;
	}

	public int getNumCols() {
		return schema.getNumCols();
	}

	/**
	 * Get the value of a specified attribute from a tuple
	 * 
	 * @param name			The name of the attribute to get
	 * @return				The value of the attribute (which may be null)
	 * @throws NameNotFound	If the the specified attribute is not in
	 * 						this tuple's schema
	 */
	public Object get(String name) throws NameNotFound {
		Integer index = schema.getColNum(name);
		if (index == null) {
			throw new NameNotFound(name, schema);
		}
		return get(index);
	}

	/**
	 * Get the value of the attribute it column <code>i</code>
	 * 
	 * @param i			The index of the attribute to get
	 * @return			The value of the attribute
	 */
	public abstract Object get(int i);

	public abstract Object getValueOrLabeledNull(int i);

	/**
	 * Get the value of the attribute in column <code>i</code>. Don't
	 * modify the returned value
	 * 
	 * @param i			The index of the attribute to get
	 * @return			The value of the attribute
	 */
	public abstract Object getNoCopy(int i);

	/**
	 * Determine if the specified column of this tuple is equal to whichever
	 * of the supplied constant and its serialized representation is easier to check
	 * 
	 * @param i				The index of the column to check
	 * @param serialized	The serialized representation
	 * @param o				The constant
	 * @return				If the column is equal to the supplied constant
	 */
	public abstract boolean equals(int i, byte[] serialized, Object o);
	
	/**
	 * Determine if two columns are equal
	 * 
	 * @param i				The index of one column
	 * @param j				The index of the other column
	 * @return				If they are equal or not
	 */
	public abstract boolean equals(int i, int j);
	
	/**
	 * Determine if a particular attribute is a labeled null in this tuple
	 * 
	 * @param name			The name of the attribute to check
	 * @return				<code>true</code> if it is, <code>false</code> if it is not
	 * @throws NameNotFound	If the specified name is not in this tuple's schema.
	 */
	public boolean isLabeledNull(String name) throws NameNotFound {
		Integer index = schema.getColNum(name);
		if (index == null) {
			throw new NameNotFound(name, schema);
		}
		return isLabeledNull(index);
	}

	/**
	 * Determine if a particular attribute is a labeled null in this tuple
	 * 
	 * @param index		The index of the attribute to check
	 * @return			<code>true</code> if it is, <code>false</code> if it is not
	 */
	public abstract boolean isLabeledNull(int index);

	public static class IsNotLabeledNull extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}

	public abstract boolean isNull(int index);

	/**
	 * Get the label for the null value for a particular attribute
	 * 
	 * @param index					The index of the attribute to check
	 * @return						The label of the null value, if applicable
	 * @throws IsNotLabeledNull		If the attribute is not set to a labeled null
	 * @throws NameNotFound			If the specified name is not in this tuple's schema
	 */
	public int getLabeledNull(String name) throws NameNotFound, IsNotLabeledNull {
		Integer index = schema.getColNum(name);
		if (index == null) {
			throw new NameNotFound(name, schema);
		}
		return getLabeledNull(index);
	}

	/**
	 * Get the label for the null value for a particular attribute
	 * 
	 * @param index					The index of the attribute to check
	 * @return						The label of the null value, if applicable
	 * @throws IsNotLabeledNull		If the attribute is not set to a labeled null
	 */
	public abstract int getLabeledNull(int index) throws IsNotLabeledNull;

	/**
	 * Gets the schema relation name
	 * 
	 * @return
	 */
	public String getRelationName() {
		return schema.getRelationName();
	}

	static final int[] hashMultipliers1 = {17, 37,  89, 157, 293, 601, 1201, 2411, 4801, 9601,  20011};
	static final int[] hashMultipliers2 = {29, 59, 113, 229, 461, 919, 1847, 3677, 7193, 14407, 29009};


	int hashCode(boolean first, int[] cols) {
		int hashval = 0;
		int multPos = 0;
		int[] mults = first ? hashMultipliers1 : hashMultipliers2;

		if (cols == null) {
			cols = schema.allColsArray;
		}
		final int numCols = cols.length;
		for (int i = 0; i < numCols; ++i) {
			int col = cols[i];
			Object o = this.getValueOrLabeledNull(col);
			if (o != null) {
				int thisColCode;
				if (o instanceof LabeledNull) {
					thisColCode = ((LabeledNull) o).getLabel();
				} else {
					try {
						thisColCode = schema.getColType(col).getHashCode(o);
					} catch (ValueMismatchException e) {
						throw new RuntimeException("Could not compute hash code due to type error", e);
					}
				}
				if (multPos >= mults.length) {
					hashval = thisColCode + 37 * hashval;
				} else {
					hashval += thisColCode * mults[multPos];
					++multPos;
				}
			}
		}
		return hashval;
	}

	public int hashCode() {
		return hashCode(true, null);
	}

	public int hashCode2() {
		return hashCode(false, null);
	}

	public int keyHashCode() {
		return hashCode(true, schema.keyColsArray);
	}

	public int keyHashCode2() {
		return hashCode(false, schema.keyColsArray);
	}

	public final int hashCode(int[] columns) {
		return hashCode(true, columns);
	}

	public final int hashCode2(int[] columns) {
		return hashCode(false, columns);
	}

	public String getProvString() {
		return "";
	}

	final public String toString() {
		StringBuffer retval = new StringBuffer(getRelationName() + "(");
		final int size = this.getNumCols();
		try {
			for (int i = 0; i < size; ++i) {
				Object val = get(i);
				if (val == null) {
					retval.append("-");
				} else {
					retval.append(schema.getColType(i).getStringRep(val));
				}
				if (i != size - 1) {
					retval.append(",");
				}
			}
		} catch (ValueMismatchException vme) {
			throw new RuntimeException(vme);
		}

		
		retval.append(")");

		return retval.toString();
	}

	public abstract boolean sameSchemaAs(AbstractTuple<S> t);
	
	public abstract boolean hasSchema(S schema);

	public abstract boolean isReadOnly();
	
	@SuppressWarnings("unchecked")
	public boolean equals(Object o) {
		if (o == null || (! (o instanceof AbstractTuple))) {
			return false;
		}
		AbstractTuple<?> t = (AbstractTuple<?>) o;
		if (! t.getSchema().quickEquals(schema)) {
			return false;
		}
		AbstractTuple<S> tt = (AbstractTuple<S>) o;
		
		return equalsOnColumns(null, tt);
	}

	public boolean equalsOnColumns(int[] cols, AbstractTuple<S> t) {
		if (! sameSchemaAs(t)) {
			return false;
		}

		if (cols == null) {
			cols = schema.allColsArray;
		}
		
		for (int col : cols) {
			Type colType = schema.getColType(col);

			Object o1 = this.getValueOrLabeledNull(col);
			Object o2 = t.getValueOrLabeledNull(col);
			if (o1 == null ^ o2 == null) {
				// One is null, the other is not
				return false;
			}
			if (o1 != null && o2 != null) {
				// Both are not null
				boolean o1ln = o1 instanceof LabeledNull;
				boolean o2ln = o2 instanceof LabeledNull;
				if (o1ln ^ o2ln) {
					// One is a labeled null, the other is not
					return false;
				}
				if (o1ln && o2ln) {
					// Both are labeled nulls
					if (! o1.equals(o2)) {
						return false;
					}
				} else {
					try {
						Integer c = colType.compareTwo(o1, o2);
						if (c == null || c != 0) {
							return false;
						}
					} catch (CompareMismatch cm) {
						throw new RuntimeException("Shouldn't get CompareMismatch between tuples of same schema", cm);
					}
				}
			}
		}

		return true;
	}

	/**
	 * Compare the key columns of two Tuples
	 * 
	 * @param t			The Tuple to compare with
	 * @return			<code>true</code> if the key columns are equal,
	 * 					<code>false</code> if they are not
	 */
	public boolean sameKey(AbstractTuple<S> t) {
		if (t == null) {
			return false;
		}

		return equalsOnColumns(schema.keyColsArray, t);
	}

	public void serialize(Document doc, Element el) {
		el.setAttribute("relation", getRelationName());
		final int numCols = schema.getNumCols();
		for (int i = 0; i < numCols; ++i) {
			Element field;
			Object o = get(i);
			if (o != null) {
				try {
					field = DomUtils.addChildWithText(doc, el, "field", schema.getColType(i).getStringRep(o));
				} catch (ValueMismatchException e) {
					throw new RuntimeException("OptimizerType error while serializing tuple", e);
				}
			} else if (isLabeledNull(i)) {
				field = DomUtils.addChild(doc, el, "field");
				field.setAttribute("labeledNull", Integer.toString(getLabeledNull(i)));
			} else {
				continue;
			}
			field.setAttribute("column", schema.getColName(i));
		}
	}

	public interface TupleFactory<S extends AbstractRelation,T extends AbstractTuple<S>> {
		T createTuple(String relationName, Object... fields) throws ValueMismatchException;
		S getSchema(String relationName);
	}

	public static <S extends AbstractRelation, T extends AbstractTuple<S>> T deserialize(Element el, TupleFactory<S,T> tf) throws XMLParseException {
		String relation = el.getAttribute("relation");
		if (relation == null) {
			throw new XMLParseException("Tuples must contain the 'relation' attribute", el);
		}
		AbstractRelation schema = tf.getSchema(relation);
		Object[] fields = new Object[schema.getNumCols()];
		
		for (Element child : DomUtils.getChildElementsByName(el, "field")) {
			String column = child.getAttribute("column");
			if (column == null) {
				throw new XMLParseException("Tuple fields must contain a 'field' attribute", child);
			}
			Integer colNum = schema.getColNum(column);
			if (colNum == null) {
				throw new XMLParseException("Field '" + column + "' does not exist in relation " + relation, child);
			}
			Object o;
			if (child.hasAttribute("labeledNull")) {
				String label = child.getAttribute("labeledNull");
				try {
					o = new LabeledNull(Integer.parseInt(label));
				} catch (NumberFormatException nfe) {
					throw new XMLParseException("Could not parse label '" + label + "'", child);
				}
			} else {
				Type type = schema.getColType(colNum);
				List<Text> textChildren = DomUtils.getChildTextNodes(child);
				if (textChildren.size() != 1) {
					throw new XMLParseException("Fields that represent a non-labeled null value should contain a single block of text", child);
				}
				o = type.fromStringRep(textChildren.get(0).getData());
			}
			fields[colNum] = o;
		}

		try {
			return tf.createTuple(relation, fields);
		} catch (ValueMismatchException e) {
			throw new XMLParseException("OptimizerType error deserializing tuple", e);
		}
	}
	
	/**
	 * Returns {@code true} if the field at position {@code index} can hold
	 * labeled nulls.
	 * 
	 * @param index
	 * @return {@code true} if the field at position {@code index} can hold
	 *         labeled nulls
	 */
	public boolean isLabeledNullable(int index) {
		return schema.getField(index).getType().isLabeledNullable();
	}

	/**
	 * Returns {@code true} if the field with label {@code label} can hold
	 * labeled nulls.
	 * 
	 * @param label
	 * @return {@code true} if the field with label {@code label} can hold
	 *         labeled nulls
	 */
	public boolean isLabeledNullable(String label) {
		return schema.getField(label).getType().isLabeledNullable();
	}
}
