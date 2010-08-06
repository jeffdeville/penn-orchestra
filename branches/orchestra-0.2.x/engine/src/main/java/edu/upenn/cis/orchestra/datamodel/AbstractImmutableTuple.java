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

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.RelationMapping;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.WriteableByteArray;

public abstract class AbstractImmutableTuple<S extends AbstractRelation> extends AbstractTuple<S> {
	private static final long serialVersionUID = 1L;

	private final boolean onlyKey;
	private byte[] data;
	private int offset;
	private int length;
	
	/**
	 * Construct a new tuple
	 * 
	 * @param t				A tuple to copy the values from
	 * @param onlyKey		<code>true</code> to copy all fields,
	 * 						<code>false</code> to copy only the key fields
	 */
	public AbstractImmutableTuple(final AbstractTuple<S> t, boolean onlyKey) {
		super(t.schema);
		this.onlyKey = onlyKey;
		offset = 0;
		Object[] constants = new Object[schema._columnLengths.length];
		for (int i = 0; i < constants.length; ++i) {
			constants[i] = t.getValueOrLabeledNull(i);
		}
		try {
			data = schema.createTupleFromConstants(constants, onlyKey);
			length = data.length;
		} catch (ValueMismatchException vme) {
			throw new IllegalStateException("Should not get a ValueMismatchException when not changing schema", vme);
		}
	}

	/**
	 * Construct a new tuple
	 * 
	 * @param schema			The schema
	 * @param onlyKey			<code>true</code> to set only key fields,
	 * 							<code>false</code> to set only key fields
	 * @param fields			An array of <code>schema.getNumCols()</code> fields
	 * 							representing the values to be stored in this tuple
	 * @throws ValueMismatchException
	 */
	public AbstractImmutableTuple(final S schema, boolean onlyKey, final Object[] fields) throws ValueMismatchException {
		super(schema);
		this.onlyKey = onlyKey;
		offset = 0;
		data = schema.createTupleFromConstants(fields, onlyKey);
		length = data.length;
	}

	/**
	 * Copy a tuple
	 * 
	 * @param t			A tuple
	 * @param onlyKey	If the new tuple should be a key-only tuple
	 */
	public AbstractImmutableTuple(final AbstractImmutableTuple<S> t, boolean onlyKey) {
		super(t.schema);
		if (onlyKey != t.onlyKey) {
			this.onlyKey = onlyKey;
			offset = 0;
			data = schema.identityMapping.createTuple(onlyKey, t.data, t.offset, t.length, t.onlyKey);
			this.length = data.length;
		} else {
			this.data = t.data;
			this.onlyKey = t.onlyKey;
			this.offset = t.offset;
			this.length = t.length;
		}
	}

	/**
	 * Construct a new tuple using a mixture of data from two existing tuples
	 * 
	 * @param schema			The schema of the new tuple
	 * @param rm				Mapping between the input relations and the output relation
	 * @param first				The first input tuple
	 * @param second			The second input tuple
	 * @throws ValueMismatchException
	 */
	public AbstractImmutableTuple(final S schema, RelationMapping rm, final AbstractImmutableTuple<S> first,
			final AbstractImmutableTuple<S> second) {
		super(schema);
		if (! (rm.validForInputTuples(first, second) && rm.validForOutputSchema(schema))) {
			throw new IllegalArgumentException("Relation mapping is not valid for supplied input tuple and schema");
		}
		this.onlyKey = false;
		offset = 0;
		data = rm.createTuple(onlyKey, first.data, first.offset, first.length, first.onlyKey, second.data, second.offset, second.length, second.onlyKey);
		length = data.length;
	}

	/**
	 * Construct a new tuple using a mixture of new data
	 * and data from an existing tuple
	 * 
	 * @param schema			The schema of the new tuple
	 * @param fss				Data telling where to take each field from (in order)
	 * @param inputTuple					The tuple to copy data from
	 * @param otherFields		The other data to use to create the new tuple
	 * @throws ValueMismatchException
	 */
	public AbstractImmutableTuple(final S schema, final AbstractRelation.FieldSource[] fss, final AbstractImmutableTuple<S> inputTuple, final Object[] otherFields) throws ValueMismatchException {
		super(schema);
		this.onlyKey = false;
		offset = 0;
		RelationMapping rm = new RelationMapping(inputTuple == null ? null : inputTuple.schema, schema, otherFields, fss);
		data = rm.createTuple(onlyKey, inputTuple.data, inputTuple.offset, inputTuple.length, inputTuple.onlyKey);
		length = data.length;
	}
	
	/**
	 * Deserialize an AbstractImmutableTuple
	 * 
	 * @param schema			The schema
	 * @param onlyKey			<code>true</code> if the serialized data contains only key columns, false otherwise
	 * @param data				The data array
	 * @param offset			The offset in the data array
	 */
	public AbstractImmutableTuple(S schema, boolean onlyKey, byte[] data, int offset, int length) {
		super(schema);
		this.onlyKey = onlyKey;
		this.data = data;
		this.offset = offset;
		this.length = length;
	}
	
	public AbstractImmutableTuple(AbstractImmutableTuple<S> tuple, boolean onlyKey, RelationMapping retainColsMapping) {
		this(tuple.schema, tuple, onlyKey, retainColsMapping);
	}

	public AbstractImmutableTuple(S schema, AbstractImmutableTuple<S> tuple, boolean onlyKey, RelationMapping oneTupleMapping) {
		super(schema);
		if (! (oneTupleMapping.validForInputTuple(tuple) && oneTupleMapping.validForOutputSchema(schema))) {
			throw new IllegalArgumentException("Relation mapping is not valid for supplied input tuple and schema");
		}
		this.onlyKey = onlyKey;
		offset = 0;
		data = oneTupleMapping.createTuple(onlyKey, tuple.data, tuple.offset, tuple.length, tuple.onlyKey);
		length = data.length;
	}
	
	@Override
	public Object get(int i) {
		return schema.get(data, onlyKey, offset, length, i);
	}

	@Override
	public int getLabeledNull(int index) throws IsNotLabeledNull {
		return schema.getLabeledNull(data, onlyKey, offset, length, index);
	}

	@Override
	public Object getNoCopy(int i) {
		return get(i);
	}

	@Override
	public Object getValueOrLabeledNull(int i) {
		if (isLabeledNull(i)) {
			return new LabeledNull(getLabeledNull(i));
		} else {
			return get(i);
		}
	}

	@Override
	public boolean isLabeledNull(int index) {
		return schema.isLabeledNull(data, onlyKey, offset, index);
	}

	@Override
	public boolean isNull(int index) {
		return schema.isNull(data, onlyKey, offset, index);
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}
	
	public int getSerializedLength(int index) {
		return schema.getFieldLength(this.data, this.onlyKey, this.offset, this.length, index);
	}
	
	public int copyInto(byte[] dest, int offset, int index) {
		if (this.isNull(index)) {
			throw new IllegalArgumentException("Field #" + index + " is null");
		}
		final int length = schema.getFieldLength(this.data, this.onlyKey, this.offset, this.length, index);
		final int start = schema.getFieldOffset(this.data, this.onlyKey, this.offset, this.length, index);
		int pos = 0;
		while (pos < length) {
			dest[offset + pos] = data[start + pos];
			++pos;
		}
		return length;
	}

	/**
	 * Write the serialized representation of this AbstractImmutableTuple.
	 * @param out			The OutputBuffer to write the serialized
	 * 						representation to
	 */
	protected void getBytes(OutputBuffer out) {
		out.writeBytes(data, offset, length);
	}
	
	/**
	 * Write the serialized representation of this AbstractImmutableTuple.
	 * @param out			The OutputBuffer to write the serialized
	 * 						representation to
	 */
	protected void getBytesNoLength(OutputBuffer out) {
		out.writeBytesNoLength(data, offset, length);
	}
	
	public int getSerializedLength() {
		return length;
	}
	
	public void putBytes(byte[] dest, int offset) {
		System.arraycopy(this.data, this.offset, dest, offset, this.length);
	}
	
	public boolean isKeyTuple() {
		return onlyKey;
	}

	public boolean equalsOnColumns(int[] cols, AbstractTuple<S> t) {
		if (! (t instanceof AbstractImmutableTuple)) {
			return super.equalsOnColumns(cols, t);
		}

		if (! sameSchemaAs(t)) {
			return false;
		}

		AbstractImmutableTuple<S> tt = (AbstractImmutableTuple<S>) t;

		if (cols == null) {
			cols = schema.allColsArray;
		}

		for (int col : cols) {
			if (! schema.equalOnColumn(this.data, this.onlyKey, this.offset, this.length, tt.data, tt.onlyKey, tt.offset, tt.length, col)) {
				return false;
			}
		}

		return true;
	}

	private int hashCode = 0;

	public int hashCode() {
		if (hashCode == 0) {
			hashCode = hashCode(true, null);
		}
		return hashCode;
	}

	int hashCode(boolean first, int[] cols) {
		return schema.getHashCode(first, cols, this.data, this.onlyKey, this.offset, this.length);
	}

	public int getColHashCode(int col) {
		return schema.getColHashCode(data, onlyKey, offset, length, col);
	}
	
	public boolean equals(Object o) {
		AbstractImmutableTuple<?> ait = (AbstractImmutableTuple<?>) o;
		if (! ait.schema.quickEquals(this.schema)) {
			return false;
		}

		final int numCols = schema.getNumCols();
		for (int i = 0; i < numCols; ++i) {	
			if (! schema.equalOnColumn(this.data, this.onlyKey, this.offset, this.length, ait.data, ait.onlyKey, ait.offset, ait.length, i)) {
				return false;
			}
		}
		
		return true;
	}
	
	public int lexicographicCompare(AbstractImmutableTuple<S> t) {
		if (! sameSchemaAs(t)) {
			throw new IllegalArgumentException("Should only compare tuples from the same schema");
		}
		final int commonLen;
		if (t.length > length) {
			commonLen = length;
		} else {
			commonLen = t.length;
		}
		for (int i = 0; i < commonLen; ++i) {
			if (this.data[this.offset + i] != t.data[t.offset + i]) {
				return this.data[this.offset + i] - t.data[t.offset + i];
			}
		}
		if (length < t.length) {
			return -1;
		} else if (length > t.length) {
			return 1;
		} else {
			return 0;
		}
	}
	
	public byte[] getUniqueBytesForColumns(int[] columns) {
		return schema.getUniqueBytesForColumns(columns, this.data, this.onlyKey, this.offset, this.length);
	}
	
	public String getStringFromSerialized() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("length=" + length + ",data=[");
		for (int i = offset; i < offset + length -1; ++i) {
			sb.append(data[i]);
			sb.append(',');
		}
		sb.append(data[offset + length - 1]);
		sb.append("]");
		return sb.toString();
	}
	
	protected void changeData(byte[] data, int offset, int length) {
		hashCode = 0;
		this.data = data;
		this.offset = offset;
		this.length = length;
	}
	
	public void changeFields(Object... fields) throws ValueMismatchException {
		this.data = schema.createTupleFromConstants(fields, this.onlyKey);
		this.offset = 0;
		this.length = data.length;
		hashCode = 0;
	}

	
	public boolean equals(int i, byte[] serialized, Object o) {
		Type t = this.schema._columnTypes[i];
		if (t.canCompareSerialized()) {
			return this.schema.equals(this.data, this.onlyKey, this.offset, this.length, i, serialized);
		} else {
			Integer comp = this.schema._columnTypes[i].compare(this, i, o);
			return (comp != null && comp == 0);
		}
	}
	
	public boolean equals(int i, int j) {
		return this.schema.equalOnColumns(this.data, this.onlyKey, this.offset, this.length, this.data, this.onlyKey, this.offset, this.length,
				i, j);
	}
	
	public void applyMapping(RelationMapping rm, WriteableByteArray dest, boolean createKeyTuple, byte[][] otherFields) {
		rm.createTuple(dest, createKeyTuple, this.data, this.offset, this.length, this.onlyKey, null, -1, -1, false, otherFields);
	}
}
