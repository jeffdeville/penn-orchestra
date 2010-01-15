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

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public abstract class AbstractMutableTuple<S extends AbstractRelation> extends AbstractTuple<S> {
	private static final long serialVersionUID = 1L;
	// The actual data held by this tuple
	private final Object fields[];
	private boolean readOnly = false;


	/**
	 * Create a new, empty tuple for the specified schema.
	 * @param schema		The schema of the relation that this tuple conforms to
	 */
	public AbstractMutableTuple(S schema) {
		super(schema);
		final int numCols = schema.getNumCols();
		fields = new Object[numCols];
	}

	/**
	 * Create a new Tuple that is a deep copy of the specified tuple
	 * 
	 * @param t		The copy
	 */
	public AbstractMutableTuple(AbstractTuple<S> t) {
		super(t.getSchema());
		final int numCols = schema.getNumCols();
		fields = new Object[numCols];
		try {
			for (int i = 0; i < numCols; ++i) {
				if (t.isLabeledNull(i)) {
					this.setLabeledNull(i, t.getLabeledNull(i));
				} else {
					Object o = t.get(i);
					if (o != null) {
						set(i,o);
					}
				}
			}
		} catch (ValueMismatchException vme) {
			throw new RuntimeException(vme);
		}
	}

	public int getNumCols() {
		return fields.length;
	}

	/**
	 * Get the value of the attribute it column <code>i</code>
	 * 
	 * @param i			The index of the attribute to get
	 * @return			The value of the attribute
	 */
	public Object get(int i) {
		Object o = fields[i];
		if (o == null || o instanceof LabeledNull) {
			return null;
		}
		return schema.getColType(i).duplicateValue(o);
	}

	public Object getValueOrLabeledNull(int i) {
		Object o = fields[i];
		if (o instanceof LabeledNull) {
			return o;
		}
		return schema.getColType(i).duplicateValue(o);
	}

	/**
	 * Get the value of the attribute in column <code>i</code>. Don't
	 * modify the returned value
	 * 
	 * @param i			The index of the attribute to get
	 * @return			The value of the attribute
	 */
	public Object getNoCopy(int i) {
		Object o = fields[i];
		if (o instanceof LabeledNull) {
			return null;
		} else {
			return o;
		}
	}
	
	/**
	 * Sets a attribute in the tuple to be a labeled null. Any value already set
	 * for that attribute will be cleared. To clear the labeled null, set the
	 * attribute to a Java <code>null</code> (or any other value) using the
	 * <code>set</code> method.
	 * 
	 * @param name			The name of the attribute to set 
	 * @param label			The label to set the attribute to
	 * @throws NameNotFound	If the specified name is not in this tuple's schema.
	 */
	public void setLabeledNull(String name, int label) throws NameNotFound {
		Integer index = schema.getColNum(name);
		if (index == null) {
			throw new NameNotFound(name, schema);
		}
		setLabeledNull(index, label);
	}

	
	/**
	 * Sets a attribute in the tuple to be a labeled null. Any value already set
	 * for that attribute will be cleared. To clear the labeled null, set the
	 * attribute to a Java <code>null</code> (or any other value) using the
	 * <code>set</code> method.
	 * 
	 * @param index			The index of the attribute to set 
	 * @param label			The label to set the attribute to
	 */
	public void setLabeledNull(int index, int label) {
		fields[index] = new LabeledNull(label);
	}

	/**
	 * Determine if a particular attribute is a labeled null in this tuple
	 * 
	 * @param index		The index of the attribute to check
	 * @return			<code>true</code> if it is, <code>false</code> if it is not
	 */
	public boolean isLabeledNull(int index) {
		Object o = fields[index];
		return (o instanceof LabeledNull);
	}

	public boolean isNull(int index) {
		Object o = fields[index];
		if (o == null) {
			return true;
		} else if (o instanceof LabeledNull) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Get the label for the null value for a particular attribute
	 * 
	 * @param index					The index of the attribute to check
	 * @return						The label of the null value, if applicable
	 * @throws IsNotLabeledNull		If the attribute is not set to a labeled null
	 */
	public int getLabeledNull(int index) throws IsNotLabeledNull {
		Object o = fields[index];
		if (!(o instanceof LabeledNull)) {
			throw new IsNotLabeledNull();
		}
		return ((LabeledNull) o).getLabel();
	}

	/**
	 * Sets the value of an attribute in a tuple. The attribute may
	 * or may not already exist.
	 * 
	 * @param name			The name of the attribute to set
	 * @param val			The value to set it to
	 * @throws NameNotFound	If the specified attribute is not in this
	 * 						tuple's schema
	 * @throws ValueMismath	If the type of <code>val</code> is not compatible
	 * 						with the specified attribute
	 */
	public void set(String name, Object val) throws NameNotFound, ValueMismatchException {
		Integer index = schema.getColNum(name);
		if (index == null) {
			throw new NameNotFound(name, schema);
		}
		set(index, val);
	}

	
	/**
	 * Sets the value of an attribute in a tuple. The attribute may
	 * or may not already exist.
	 * 
	 * @param index			The index of the attribute to set
	 * @param val			The value to set it to
	 * @throws ValueMismatchException
	 * 						If the type of <code>val</code> is not compatible
	 * 						with the specified attribute
	 */
	public void set(int index, Object val) throws ValueMismatchException {
		if (readOnly) {
			throw new IllegalStateException("Attempt to modify a read-only Tuple");
		}
		if (! (val instanceof LabeledNull)) {
			Type colType = schema.getColType(index);
			if (! colType.isValidForColumn(val)) {
				throw new ValueMismatchException(val, this.schema, index);
			}
		}
		fields[index] =  val;
	}
	
	/**
	 * Determine if a tuple consists of entirely null fields. We treat such a
	 * tuple as being a null tuple (i.e. the old value in an insertion or the
	 * new value in a deletion). This means that such a tuple cannot be inserted
	 * or deleted from the shared database (which is probably fine).
	 * 
	 * @return <code>true</code> if it does, <code>false</code> if it does not.
	 */
	public boolean isNull() {
		for (Object o : fields) {
			if (o != null) {
				return false;
			}
		}
		return true;
	}

	public void setReadOnly() {
		readOnly = true;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public boolean equals(int i, byte[] serialized, Object o) {
		Integer comp = this.schema._columnTypes[i].compare(this, i, o);
		return (comp != null && comp == 0);
	}
	
	public boolean equals(int i, int j) {
		if (fields[i] instanceof LabeledNull) {
			return fields[i].equals(fields[j]);
		} else if (fields[j] instanceof LabeledNull) {
			return false;
		}
		Integer comp = this.schema._columnTypes[i].compare(this, i, j);
		return (comp != null && comp == 0);
	}
}
