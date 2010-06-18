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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.collect.ImmutableSortedSet;

import edu.upenn.cis.orchestra.datamodel.AbstractTuple.IsNotLabeledNull;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.BitSet;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.WriteableByteArray;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * The data layout for an AbstractImmutableTuple is as follows:
 * 
 * 1. Null & labeled null field flags (ceil((n+m)/8) bytes, where n is the
 *    number of nullable fields and m is the number of labeled nullable fields)
 * 2. Fixed-length section
 * 			For each field, in order,
 * 				a. if the field is fixed length
 * 					i) if the field is null, zeros
 * 					ii) if the field is a labeled null, the value of the null
 * 					iii) otherwise, the content of the field
 * 				b. if the field is variable length
 * 					i) if the field is null, the 32-bit offset into the tuple
 * 						of the start of the next non-null field
 * 					ii) if the field is a labeled null, the 32-bit offset into
 * 						the tuple of the value of the labeled null
 * 					iii) otherwise, the 32-bit offset into the tuple of the
 * 						value of the field
 * 				Note that this means that no labeled nullable fixed-length data element can have a length
 * 				less than the length of a labeled null (currently a 4-byte integer). Also, not that
 * 				the lengths of the variable-length fields are implicitly given by the next offset
 * 				or the end of the tuple.
 * 3. Variable-length section
 * 			The pointers from section 3 give the start and end of the
 * 			various fields
 * 
 *  If the tuple is a key tuple, all of the non-key attributes are omitted
 *  from all data structures, including the null and labeled null flags
 * 
 * @author netaylor
 *
 */
public abstract class AbstractRelation implements Serializable {
	private static final long serialVersionUID = 1L;

	public static class BadColumnName extends Exception {
		private static final long serialVersionUID = 1L;

		BadColumnName(String name) {
			super("Column name '" + name + "' is not valid");
		}
	}

	public static class NameNotFound extends Exception {
		private static final long serialVersionUID = 1L;

		public NameNotFound(String name, AbstractRelation schema) {
			super("Attribute '" + name + "' not found in tuple schema " + schema);
		}
	}

	public class SchemaMismatch extends Exception {
		private static final long serialVersionUID = 1L;

		SchemaMismatch(Relation tupleSchema) {
			super("Attempt to perform operation between tuple with schema " + AbstractRelation.this +
					" and tuple with schema " + tupleSchema);
		}
	}

	/** Relation name */	
	protected String _name;
	/** Relation description */
	protected String _description;
	/** Relation fields */
	private List<RelationField> _fields;
	// An unmodifiable copy of _fields, created when relation is
	// finished
	private List<RelationField> _publicFields = null;

	/** Map fields name to the field object to avoid scanning _fields for access by name, which will be frequent */
	protected Map<String, RelationField> _fieldsByName;


	/*** Constraints. 
	 * Note that constraints are not stored as a list of ScConstraint to avoid scanning the list for Pk, 
	 * casting too often... 
	 * If check constraint ... are to be added, they will be stored as a list of ScConstraint or a new abstract subclass
	 * to avoid adding Pk... to this list
	 */
	/** Relation's primary key, can stay null */
	protected PrimaryKey _pk=null;
	/** Relation foreign keys */
	protected List<ForeignKey> _foreignKeys = new ArrayList<ForeignKey> ();
	/** Relation unique indexes */
	protected List<RelationIndexUnique> _uniqueIndexes = new ArrayList<RelationIndexUnique> ();
	/** Non unique indexes */
	protected List<RelationIndexNonUnique> _nonUniqueIndexes = new ArrayList<RelationIndexNonUnique> ();

	// columnTypes[i] contains the type of the ith attribute
	Type[] _columnTypes;
	// columnLength[i] gives the length of the serialization of the ith column,
	// from _columnTypes[i].bytesLength()
	int[] _columnLengths;
	// getColumnNum[name] =	gives the index of the type of the column
	//							with called name

	private HashMap<String,Integer> _columnNum;

	/** True if we've finished creating this schema and it won't be modified in the future. */
	protected boolean finished;

	private SortedSet<Integer> _keyColumnList;

	public AbstractRelation() {
		this((String) null);
	}

	int[] keyColsArray, allColsArray;

	public AbstractRelation(String name) {
		_name = name; 
		_columnNum = new HashMap<String,Integer>();
		finished = false;
		_fields = new ArrayList<RelationField> ();
		_columnTypes = new Type[_fields.size()];

		// TODO: raise exception if fields.size=0
		initFieldsByNamesMap ();
	}

	/**
	 * Creates a new relation
	 * @param name Relation name
	 * @param description Relation description
	 * @param fields Relation fields
	 */
	public AbstractRelation (String name, 
			String description, List<RelationField> fields) 
	{
		_columnNum = new HashMap<String,Integer>();
		finished = false;

		// TODO: raise exception if fields.size=0
		_name = name;
		_description = description;
		_fields = new ArrayList<RelationField> ();
		_fields.addAll(fields);
		_columnTypes = new Type[_fields.size()];

		setFieldsRelRef();
		initFieldsByNamesMap ();
	}

	/**
	 * Creates a new relation
	 * @param name Relation name
	 * @param description Relation description
	 * @param pkName The primary key constraint name
	 * @param pkFieldNames The names of the fields in the primary key
	 * @throws UnknownRefFieldException 
	 */
	public AbstractRelation(String name, String description,
			List<RelationField> fields, String pkName,
			Collection<String> pkFieldNames) throws UnknownRefFieldException {
		_columnNum = new HashMap<String,Integer>();
		finished = false;

		// TODO: raise exception if fields.size=0
		_name = name;
		_description = description;
		_fields = new ArrayList<RelationField> ();
		_fields.addAll(fields);
		_columnTypes = new Type[_fields.size()];

		setFieldsRelRef();
		initFieldsByNamesMap ();

		if (pkName != null && pkFieldNames != null) {
			_pk = new PrimaryKey(pkName,this,pkFieldNames);
		}

		markFinished();

	}

	/**
	 * Deep copy of the relation
	 * Use the method deepCopy to benefit from polymorphism
	 * @param relation Relation to copy
	 * @roseuid 449AEA650271
	 * @see AbstractRelation#deepCopy()
	 */
	protected AbstractRelation(AbstractRelation relation) 
	{
		_name = relation.getName();
		_description = relation.getDescription();
		_fields = new ArrayList<RelationField> ();
		_columnNum = new HashMap<String,Integer>();
		int i = 0;
		for (RelationField fld : relation._fields){
			_fields.add(fld.deepCopy ());
			_columnNum.put(fld.getName(), i++);
		}

		_columnTypes = new Type[relation._columnTypes.length];
		for(int j = 0; j < relation._columnTypes.length; j++)
			_columnTypes[j] = relation.getColType(j);

		_columnLengths = new int[relation._columnLengths.length];
		for(int j = 0; j < relation._columnLengths.length; j++)
			_columnLengths[j] = relation._columnLengths[j];

		setFieldsRelRef();
		initFieldsByNamesMap ();
		
		initKeyColumnList(relation._keyColumnList);
		initAllColsArray();
	}   

	/**
	 * Gets the number of columns in the schema.
	 * 
	 * @return			The number of columns
	 */
	public int getNumCols() {
		return _fields.size();
	}

	/**
	 * Get the name of a column.
	 * 
	 * @param whichCol			The index of the column of interest
	 * @return					The name of the column, or <code>null</code>
	 * 							if it does not exist
	 */
	public String getColName(int whichCol) {
		try {
			return _fields.get(whichCol).getName();
		} catch (IndexOutOfBoundsException ioobe) {
			return null;
		}
	}

	/**
	 * Get the number of a column
	 * 
	 * @param whichCol			The name of the column of interest
	 * @return					The index of the column, or <code>null</code>
	 * 							if it does not exist
	 */
	public Integer getColNum(String whichCol) {
		return _columnNum.get(whichCol);
	}

	/**
	 * Get the type of a column
	 * 
	 * @param whichCol			The index of the column of interest
	 * @return					The <CODE>OptimizerType</CODE> of the column
	 */
	public Type getColType(int whichCol) {
		if (_columnTypes[whichCol] != null)
			return _columnTypes[whichCol];
		else {
			return _fields.get(whichCol).getType();
		}
	}

	/**
	 * Get the type of a column
	 * 
	 * @param whichCol			The name of the column of interest
	 * @return					The <CODE>OptimizerType</CODE> of the column
	 */
	public Type getColType(String whichCol) {
		int inx = _columnNum.get(whichCol);
		if (_columnTypes[inx] != null)
			return _columnTypes[inx];
		else
			return _fields.get(inx).getType();
	}

	// Number of bytes given over to flags in key and full tuples
	private int numKeyFlagBytes = 0, numFlagBytes = 0;

	// For key a full tuples, mappings from column indexes to indexes into the BitSet
	// that indicates if columns are null or labeled null
	// -1 indicates that a column is not present or cannot be a (labeled) null
	private int[] keyTupleNullFlags, keyTupleLabeledNullFlags, nullFlags, labeledNullFlags;

	// Offsets into key and full tuples of the starts of the fixed-length
	// section of the tuple for the specified columns
	// -1 indicates that a column is not present
	private int[] keyFieldOffsets, fieldOffsets;

	// Offset of the offset of the next variable field after a variable field, or
	// Integer.MIN_VALUE if the current variable field is the last variable field.
	// -1 indicates that the current field is either not present or not variable
	// Useful for figuring out the end of a variable field
	private int[] keyNextVariableFieldOffsetOffset, nextVariableFieldOffsetOffset;

	// Length of the fixed parts of a key tuple and a full tuple (i.e. the part before the variable length fields)
	private int keyTupleFixedLength, fullTupleFixedLength;

	RelationMapping identityMapping;

	public void markFinished() {
		if (finished) {
			throw new IllegalStateException("Already finished");
		}
		if (_pk == null) {
			List<String> cols = new ArrayList<String>(_fields.size());
			for (RelationField f : _fields) {
				cols.add(f.getName());
			}
			PrimaryKey pk;
			try {
				pk = new PrimaryKey("pk", this, cols);
			} catch (UnknownRefFieldException e) {
				throw new RuntimeException("Should not happen", e);
			}
			setPrimaryKey(pk);
		}
		finished = true;

		initKeyColumnList();
		_publicFields = Collections.unmodifiableList(_fields);

		_columnTypes = new Type[_fields.size()];
		_columnLengths = new int[_fields.size()];
		for (int i = 0; i < _columnTypes.length; ++i) {
			_columnTypes[i] = _fields.get(i).getType();
			_columnLengths[i] = _columnTypes[i].bytesLength();
		}

		keyColsArray = new int[_keyColumnList.size()];
		
		initAllColsArray();
		int pos = 0;
		for (int col : _keyColumnList) {
			keyColsArray[pos++] = col;
		}

		keyTupleNullFlags = new int[allColsArray.length];
		keyTupleLabeledNullFlags = new int[allColsArray.length];
		nullFlags = new int[allColsArray.length];
		labeledNullFlags = new int[allColsArray.length];
		int numKeyFlags = 0, numFlags = 0;
		keyFieldOffsets = new int[_columnTypes.length];
		fieldOffsets = new int[_columnTypes.length];
		keyNextVariableFieldOffsetOffset = new int[_columnTypes.length];
		nextVariableFieldOffsetOffset = new int[_columnTypes.length];

		for (int i = 0; i < allColsArray.length; ++i) {
			keyTupleNullFlags[i] = -1;
			keyTupleLabeledNullFlags[i] = -1;
			nullFlags[i] = -1;
			labeledNullFlags[i] = -1;
			keyFieldOffsets[i] = -1;
			nextVariableFieldOffsetOffset[i] = -1;
			keyNextVariableFieldOffsetOffset[i] = -1;
		}

		for (int col : _keyColumnList) {
			if (_columnTypes[col].isNullable()) {
				keyTupleNullFlags[col] = numKeyFlags++;
			}
			if (_columnTypes[col].isLabeledNullable()) {
				keyTupleLabeledNullFlags[col] = numKeyFlags++;
			}
		}

		for (int col = 0; col < _columnTypes.length; ++col) {
			Type t = _columnTypes[col];
			if (t.isNullable()) {
				nullFlags[col] = numFlags++;
			}
			if (t.isLabeledNullable()) {
				labeledNullFlags[col] = numFlags++;
			}
		}

		numFlagBytes = numFlags / 8;
		if (numFlags % 8 != 0) {
			++numFlagBytes;
		}
		numKeyFlagBytes = numKeyFlags / 8;
		if (numKeyFlags % 8 != 0) {
			++numKeyFlagBytes;
		}

		int keyTupleOffset = this.numKeyFlagBytes;
		int lastVariableField = -1;
		for (int col : _keyColumnList) {
			Type t = _columnTypes[col];
			int bytesLength = t.bytesLength();
			if (bytesLength >= 0) {
				if (t.isLabeledNullable() && bytesLength < IntType.bytesPerInt) {
					bytesLength = IntType.bytesPerInt;
				}
			} else {
				if (lastVariableField >= 0) {
					keyNextVariableFieldOffsetOffset[lastVariableField] = keyTupleOffset;
				}
				bytesLength = IntType.bytesPerInt;
				lastVariableField = col;
			}
			keyFieldOffsets[col] = keyTupleOffset;
			keyTupleOffset += bytesLength;
		}
		keyTupleFixedLength = keyTupleOffset;
		if (lastVariableField >= 0) {
			keyNextVariableFieldOffsetOffset[lastVariableField] = Integer.MIN_VALUE;
		}

		int fullTupleOffset = this.numFlagBytes;
		lastVariableField = -1;
		for (int col = 0; col < _columnTypes.length; ++col) {
			Type t = _columnTypes[col];
			int bytesLength = t.bytesLength();
			if (bytesLength >= 0) {
				if (t.isLabeledNullable() && bytesLength < IntType.bytesPerInt) {
					bytesLength = IntType.bytesPerInt;
				}
			} else {
				if (lastVariableField >= 0) {
					nextVariableFieldOffsetOffset[lastVariableField] = fullTupleOffset;
				}
				bytesLength = IntType.bytesPerInt;
				lastVariableField = col;
			}
			fieldOffsets[col] = fullTupleOffset;
			fullTupleOffset += bytesLength;			
		}
		fullTupleFixedLength = fullTupleOffset;
		if (lastVariableField >= 0) {
			nextVariableFieldOffsetOffset[lastVariableField] = Integer.MIN_VALUE;
		}

		identityMapping = new RelationMapping(this);
	}

	/**
	 * DOCUMENT ME
	 * 
	 */
	private void initAllColsArray() {
		allColsArray = new int[_fields.size()];
		for (int i = 0; i < allColsArray.length; ++i) {
			allColsArray[i] = i;
		}
	}

	
	private void initKeyColumnList() {
		ImmutableSortedSet.Builder<Integer> keyColumnBuilder = ImmutableSortedSet.naturalOrder();

		for (RelationField f : (_pk == null ? _fields : _pk.getFields())) {
			keyColumnBuilder.add(_columnNum.get(f.getName()));
		}
		_keyColumnList = keyColumnBuilder.build();
	}
	
	private void initKeyColumnList(SortedSet<Integer> keyColumnList) {
		ImmutableSortedSet.Builder<Integer> keyColumnBuilder = ImmutableSortedSet.naturalOrder();

		for (Integer column : keyColumnList) {
			keyColumnBuilder.add(column);
		}
		_keyColumnList = keyColumnBuilder.build();
	}

	/**
	 * Get the column numbers of the key columns, in ascending order
	 * 
	 * @return			A set of the key column numbers, which is immutable
	 */
	public SortedSet<Integer> getKeyCols() {
		return _keyColumnList;
	}

	public boolean isFinished() {
		return finished;
	}

	//@Override
	public boolean equalsREMOVE(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		AbstractRelation ts = (AbstractRelation) o;

		return equals(ts, true);

	}

	//@Override
	public int hashCodeREMOVE() {
		int code = 17;
		code = 31 * code + getRelationName().hashCode();
		for (RelationField field : _fields) {
			code = 31 * code + field.getName().hashCode();
			Type t = field.getType();
			code = 31 * code + t.hashCode();
		}
		return code;
	}

	public boolean equalsOnColumns(AbstractRelation ts) {
		return equals(ts, false);
	}

	public abstract boolean quickEquals(AbstractRelation ar);

	private boolean equals(AbstractRelation ts, boolean includeName) {
		if (includeName && (! getRelationName().equals(ts.getRelationName()))) {
			return false;
		}

		final int numCols = getNumCols();

		if (ts._fields.size() != numCols) {
			return false;
		}

		for (int i = 0; i < numCols; ++i) {
			Type t1 = _fields.get(i).getType();
			Type t2 = ts._fields.get(i).getType();
			if (! _fields.get(i).getName().equals(ts._fields.get(i).getName())) {
				return false;
			}
			if (! t1.equals(t2)) {
				return false;
			}
		}

		return true;
	}
	
	/**
	 * Init the map used to access fields by name
	 */
	private void initFieldsByNamesMap ()
	{
		_fieldsByName = new Hashtable<String, RelationField> ();
		int i = 0;
		for (RelationField fld : _fields) {
			_fieldsByName.put(fld.getName(), fld);
			_columnNum.put(fld.getName(), i++);
		}
	}

	private void setFieldsRelRef(){
		for(RelationField field : _fields)
			field.setRelation(this);
	}


	/**
	 * Add a new column to the current schema
	 * 
	 * @param name		The name of the column to add
	 * @param se		The column to add
	 * @throws Exception	If the column name is reserved
	 */
	public void addCol(String name, Type se) throws BadColumnName {
		addCol(name, "column", se);
	}

	/**
	 * Add a new column to the current schema
	 * 
	 * @param name		The name of the column to add
	 * @param desc		The column description
	 * @param se		The column to add
	 * @throws Exception	If the column name is reserved
	 */
	public void addCol(String name, String desc, Type se) throws BadColumnName {
		if (finished) {
			throw new RuntimeException("Attempt to modify finished schema");
		}
		// TODO: move these checks into reconciliation code
		if (name.equals("peer") || name.equals("recno") || name.equals("tid") ||
				name.equals("conflict") || name.equals("option") ||
				name.equals("tid1") || name.equals("tid2") ||
				name.equals("serialno") || name.equals("txn") ||
				name.equals("truster") ||  name.equals("trusted") ||
				name.endsWith("_old") || name.endsWith("_new")) {
			throw new BadColumnName(name);
		}
		addField(new RelationField(name, desc, se));
	}

	/**
	 * Add a new field to this relation, inserted at a given index (to respect the 
	 * source database order)
	 * @param f Field to be added
	 * @param index Index at which the field is to be inserted
	 * @roseuid 449AE4D401A5
	 */
	public void addField(RelationField f, int index) throws BadColumnName 
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify finished schema");
		}
		//	TODO: LOW Error if index KO
		_fields.add (index, f);
		_columnNum.put(f.getName(), index);
		_fieldsByName.put(f.getName(), f);
	}

	/**
	 * Add a new field at the end of the relation
	 * @param f New field
	 */
	public void addField(RelationField f) throws BadColumnName
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify finished schema");
		}
		if (_fieldsByName.containsKey(f.getName())) {
			throw new IllegalArgumentException("Field '" + f.getName() + "' is alreay in relation " + _name);
		}
		_fields.add (f);
		_columnNum.put(f.getName(), _fields.size() - 1);
		_fieldsByName.put(f.getName(), f);
	}

	/**
	 * Get the fields contained in this relation.
	 * To optimize performance this is not a deep copy. DO NOT MODIFY.
	 * @return List
	 * @roseuid 449AE61A001F
	 */
	public List<RelationField> getFields() 
	{
		if (_publicFields != null) {
			return _publicFields;
		} else {
			return Collections.unmodifiableList(_fields);
		}
	}

	/**
	 * Get a field by index
	 * @param index Index of the field to retrieve
	 * @return Field if found TODO : LOW Exception if not found
	 */
	public RelationField getField (int index)
	{
		//	TODO:LOW error if ind KO
		return _fields.get(index);
	}

	/**
	 * Get a field by name
	 * @param name Name of the field to retrieve
	 * @return Field if found, null if it does not exist
	 */
	public RelationField getField (String name)
	{
		return _fieldsByName.get(name);

	}

	/**
	 * Get the name of this relation
	 * @return java.lang.String Relation name
	 * @roseuid 44AD2CC800AB
	 */
	public String getName() 
	{
		return _name;
	}

	/**
	 * Get the description of this relation
	 * @return java.lang.String Relation description
	 * @roseuid 44AD2CCF037A
	 */
	public String getDescription() 
	{
		return _description;
	}

	/**
	 * Change the relation description
	 * @param description New description
	 */
	public void setDescription (String description)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_description = description;
	}


	/**
	 * Set the primary key for this relation. The primary key cannot
	 * be changed once set
	 * @param pk primary key, fields must exist in the relation
	 */
	public void setPrimaryKey (PrimaryKey pk)
	{
		if (_pk != null) {
			throw new IllegalStateException("Cannot change the primary key once it is set");
		}
		if (pk == null) {
			throw new NullPointerException();
		}
		for (RelationField f : pk.getFields()) {
			RelationField found = getField(f.getName());
			if (found == null || (! found.equals(f))) {
				throw new IllegalArgumentException("Fields in primary key must match those in relation");
			}
		}
		_pk = pk;
	}

	/**
	 * Set the primary key for this relation. The primary key cannot
	 * be changed once set
	 * @param pkName		The primary key name
	 * @param keyColNames	A collection of fields in this relation
	 * @throws UnknownRefFieldException
	 */
	public void setPrimaryKey(String pkName, Collection<String> keyColNames) throws UnknownRefFieldException {
		this.setPrimaryKey(new PrimaryKey(pkName, this, keyColNames));
	}

	/**
	 * Set the primary key for this relation. The primary key cannot
	 * be changed once set
	 * @param keyColNames	A collection of fields in this relation
	 * @throws UnknownRefFieldException
	 */
	public void setPrimaryKey(Collection<String> keyColNames) throws UnknownRefFieldException {
		this.setPrimaryKey(new PrimaryKey("pk", this, keyColNames));
	}

	/**
	 * Add a foreign key to this relation
	 * Referenced table fields / pk coherency is not checked
	 * @param fk New foreign key, fields must exist in the relation.
	 */
	public void addForeignKey (ForeignKey fk)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		if (! fk.getRelation().equals(_name)) {
			throw new IllegalArgumentException("Foreign key is not for this relation");
		}
		// Check that fields exist and have the same type etc.
		for (RelationField sf : fk._fields) {
			RelationField inRel = _fieldsByName.get(sf.getName());
			if (inRel == null) {
				throw new IllegalArgumentException("Relation does not contain fields " + sf.getName());
			} else if (! sf.equals(inRel)) {
				throw new IllegalArgumentException("Field in relation is not the same as field in foreign key");
			}
		}
		_foreignKeys.add(fk);
	}

	/**
	 * Remove a given foreign key from the relation.
	 * Will not raise an exception if the foreign key is not found
	 * @param fk Foreign key to remove
	 */
	public void removeForeignKey (ForeignKey fk)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_foreignKeys.remove(fk);
	}

	/**
	 * Remove all foreign keys
	 *
	 */
	public void clearForeignKeys ()
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_foreignKeys.clear();
	}

	/**
	 * Add a non unique index to this relation
	 * @param idx New "non unique" index, fields must exist in the relation
	 */
	public void addNonUniqueIndex (RelationIndexNonUnique idx)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		//	TODO:MID Check that fields exist
		_nonUniqueIndexes.add (idx);
	}

	/**
	 * Remove a given non unique index
	 * @param idx Non unique index to remove
	 */
	public void removeNonUniqueIndex (RelationIndexNonUnique idx)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_nonUniqueIndexes.remove (idx);
	}

	/**
	 * Remove all non unique indexes from the relation
	 *
	 */
	public void clearNonUniqueIndexes ()
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_nonUniqueIndexes.clear();
	}

	/**
	 * Add a unique index to this relation
	 * @param idx New unique index, fields must exist in the relation
	 */
	public void addUniqueIndex (RelationIndexUnique idx)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		//	TODO:MID Check that fields exist
		_uniqueIndexes.add (idx);
	}

	/**
	 * Remove a given unique index
	 * @param idx Unique index to remove
	 */
	public void removeUniqueIndex (RelationIndexUnique idx)
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_uniqueIndexes.remove (idx);
	}

	/**
	 * Remove all unique indexes from the relation
	 *
	 */
	public void clearUniqueIndexes ()
	{
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_uniqueIndexes.clear();
	}	

	/**
	 * Returns the primary key
	 * @return Primary key, null if none
	 */
	public PrimaryKey getPrimaryKey ()
	{
		return _pk;
	}

	/**
	 * Returns the list of unique indexes.
	 * @return Unique indexes
	 */
	public List<RelationIndexUnique> getUniqueIndexes ()
	{
		return Collections.unmodifiableList(_uniqueIndexes);
	}

	/**
	 * Returns the list of NON unique indexes.
	 * @return Non unique indexes
	 */
	public List<RelationIndexNonUnique> getNonUniqueIndexes ()
	{
		return Collections.unmodifiableList(_nonUniqueIndexes);
	}	

	/**
	 * Returns the list of foreign keys (from this relation to another ot to itself)
	 * @return Foreign keys
	 */
	public List<ForeignKey> getForeignKeys ()
	{
		return Collections.unmodifiableList(_foreignKeys);
	}

	// Alias to TableSchema version
	public String getRelationName() {
		return getName();
	}

	/// Original TableSchema version
	public String toStringText() {
		StringBuffer retval = new StringBuffer(getRelationName() + "(\n");

		int size = _fields.size();
		for (int i = 0; i < size; ++i) {
			String name = _fields.get(i).getName();
			retval.append(name + "\t" + _fields.get(i).getType().getSQLType());
			if (i != (size - 1)) {
				retval.append(",");
			}
			retval.append("\n");
		}
		retval.append(", PRIMARY KEY (");
		Iterator<Integer> colIt = _keyColumnList.iterator();
		while (colIt.hasNext()) {
			retval.append(_fields.get(colIt.next()).getName());
			if (colIt.hasNext()) {
				retval.append(",");
			}
		}
		retval.append(")\n");

		retval.append(")\n");
		return retval.toString();
	}

	public void serialize(Document doc, Element schema) {
		schema.setAttribute("name", _name);
		if (_description != null) {
			schema.setAttribute("description", _description);
		}
		for (RelationField f : getFields()) {
			Element field = DomUtils.addChild(doc, schema, "field");
			f.serialize(doc, field);
		}
		if (_pk != null) {
			Element pk = DomUtils.addChild(doc, schema, "primaryKey");
			pk.setAttribute("name", _pk.getName());
			for (RelationField f : _pk.getFields()) {
				Element fieldEl = DomUtils.addChild(doc, pk, "fieldName");
				fieldEl.setAttribute("name", f.getName());
			}
		}
		for (ForeignKey fk : _foreignKeys) {
			Element foreignkey = DomUtils.addChild(doc, schema, "foreignKey");
			foreignkey.setAttribute("name", fk.getName());
			foreignkey.setAttribute("refRel", fk._refRelation._name);
			Iterator<RelationField> refFields = fk._refFields.iterator();
			Iterator<RelationField> fields = fk._fields.iterator();

			while (refFields.hasNext()) {
				String refField = refFields.next().getName();
				String thisField = fields.next().getName();

				Element entry = DomUtils.addChild(doc, foreignkey, "fkEntry");
				entry.setAttribute("refField", refField);
				entry.setAttribute("field", thisField);
			}
		}

	}

	protected static class AbstractRelationInfo {
		public final String name;
		private final String descr;
		private final List<RelationField> fields;
		private final String pkName;
		private final Collection<String> pkFieldNames;
		private final Collection<FkInfo> fks;

		AbstractRelationInfo(String name, String descr, List<RelationField> fields,
				String pkName, Collection<String> pkFieldNames, Collection<FkInfo> fks) {
			this.name = name;
			this.descr = descr;
			this.fields = fields;
			this.pkName = pkName;
			this.pkFieldNames = pkFieldNames;
			this.fks = fks;
		}

		public void decode(AbstractRelation ar, GetSchema<?> gs) throws UnknownRefFieldException, BadColumnName {
			ar.setDescription(descr);
			for (RelationField f : fields) {
				ar.addField(f);
			}
			if (pkName != null) {
				ar.setPrimaryKey(pkName, pkFieldNames);
			}
			for (FkInfo fki : fks) {
				fki.decode(ar,gs);
			}
		}
	}

	public interface GetSchema<S extends AbstractRelation> {
		S getSchema(String name);
	}

	private static class FkInfo {
		private final String fkName;
		private final String refRelName;
		private final List<String> refFieldNames;
		private final List<String> thisFieldNames;

		FkInfo(String fkName, String refRelName, List<String> refFieldNames, List<String> thisFieldNames) {
			this.fkName = fkName;
			this.refRelName = refRelName;
			this.refFieldNames = refFieldNames;
			this.thisFieldNames = thisFieldNames;
		}

		void decode(AbstractRelation ar, GetSchema<?> gs) throws UnknownRefFieldException {
			AbstractRelation ref = gs.getSchema(refRelName);
			if (ref == null) {
				throw new IllegalArgumentException("No relation found for " + refRelName);
			}
			ForeignKey fk = new ForeignKey(fkName, ar, thisFieldNames, ref, refFieldNames);
			ar.addForeignKey(fk);
		}
	}

	public static AbstractRelationInfo deserializeAbstractRelation(Element relElt) throws XMLParseException, UnknownRefFieldException, UnsupportedTypeException {
		String name = relElt.getAttribute("name");
		if (name.length() == 0) {
			throw new XMLParseException("Missing relation name", relElt);
		}
		String descr = relElt.getAttribute("description");
		if (descr.length() == 0) {
			descr = null;
		}

		ArrayList<RelationField> fields = new ArrayList<RelationField>();
		for (Element field : DomUtils.getChildElementsByName(relElt, "field")) {
			RelationField f = RelationField.deserialize(field);
			fields.add(f);
		}
		Element primaryKey = DomUtils.getChildElementByName(relElt, "primaryKey");
		String pkName = null;
		List<String> pkFieldNames = null;
		if (primaryKey != null) {
			pkName = primaryKey.getAttribute("name");
			pkFieldNames = new ArrayList<String>();
			if (pkName.length() == 0) {
				throw new XMLParseException("Missing primary key name", primaryKey);
			}
			for (Element fieldEl : DomUtils.getChildElementsByName(primaryKey, "fieldName")) {
				String fieldName = fieldEl.getAttribute("name");
				if (fieldName.length() == 0) {
					throw new XMLParseException("Missing name for field", fieldEl);
				}
				pkFieldNames.add(fieldName);
			}
		}

		List<FkInfo> fkInfo = new ArrayList<FkInfo>();
		List<Element> fks = DomUtils.getChildElementsByName(relElt, "foreignKey");
		for (Element fk : fks) {
			String fkName = fk.getAttribute("name");
			if (fkName.length() == 0) {
				throw new XMLParseException("Missing foreign key name", fk);
			}
			String refRel = fk.getAttribute("refRel");
			if (refRel.length() == 0) {
				throw new XMLParseException("Missing referenced relation name", fk);
			}
			List<String> refFields = new ArrayList<String>();
			List<String> thisFields = new ArrayList<String>();
			List<Element> fkEntries = DomUtils.getChildElementsByName(fk, "fkEntry");
			for (Element fkEntry : fkEntries) {
				String refField = fkEntry.getAttribute("refField");
				if (refField.length() == 0) {
					throw new XMLParseException("Missing referenced field name", fkEntry);
				}
				String thisField = fkEntry.getAttribute("field");
				if (thisField.length() == 0) {
					throw new XMLParseException("Missing field name", fkEntry);
				}
			}
			fkInfo.add(new FkInfo(fkName, refRel, refFields, thisFields));
		}

		return new AbstractRelationInfo(name, descr, fields, pkName,
				pkFieldNames, fkInfo);
	}

	/**
	 * Given a serialized representation of a tuple, determine if one of its
	 * columns is a labeled null
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param index					The index of the desired column
	 * @return						<code>true</code> if it is a labeled null, <code>false</code> if it is not
	 */
	final public boolean isLabeledNull(byte[] serialized, boolean onlyKey, int offset, int index) {
		int indexInBitset = onlyKey ? this.keyTupleLabeledNullFlags[index] : this.labeledNullFlags[index];
		if (indexInBitset < 0) {
			// Field cannot be a labeled null
			return false;
		}
		return BitSet.getField(indexInBitset, serialized, offset);
	}

	/**
	 * Given a serialized representation of a tuple, determine if one of its
	 * columns is null (and not a labeled null)
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param index					The index of the desired column
	 * @return						<code>true</code> if it is a non-labeled null, <code>false</code> if it is not
	 */
	final public boolean isRegularNull(byte[] serialized, boolean onlyKey, int offset, int index) {
		int indexInBitset;
		if (onlyKey) {
			indexInBitset = this.keyTupleNullFlags[index];
			if (indexInBitset < 0) {
				// Field is not present or cannot be null
				// Return if field is not present
				return this.keyFieldOffsets[index] < 0;
			}
		} else {
			indexInBitset = this.nullFlags[index];
			if (indexInBitset < 0) {
				// Field is present and cannot be null
				return false;
			}
		}
		return BitSet.getField(indexInBitset, serialized, offset);
	}

	/**
	 * Given a serialized representation of a tuple, determine if one of its
	 * columns is null (regular null or labeled null)
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param index					The index of the desired column
	 * @return						<code>true</code> if it is either kind of null, <code>false</code> if it is not
	 */
	final public boolean isNull(byte[] serialized, boolean onlyKey, int offset, int index) {
		return isRegularNull(serialized, onlyKey, offset, index) || isLabeledNull(serialized, onlyKey, offset, index);
	}

	/**
	 * Given a serialized representation of a tuple, get a labeled null value from one of its columns
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param tupleLength			The length of the serialized tuple
	 * @param index					The index of the desired column
	 * @return						The value of the labeled null for that column
	 * @throws IsNotLabeledNull		If the column is not a labeled null in the supplied tuple
	 */
	final public int getLabeledNull(byte[] serialized, boolean onlyKey, int offset, int tupleLength, int index) throws IsNotLabeledNull {
		if (! isLabeledNull(serialized, onlyKey, offset, index)) {
			throw new IsNotLabeledNull();
		}
		int fieldOffset = this.getFieldOffset(serialized, onlyKey, offset, tupleLength, index);
		return IntType.getValFromBytes(serialized, fieldOffset);
	}

	/**
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param tupleOffset			The offset in the <code>serialized</code> array of the start of the tuple
	 * @param tupleLength			The length of the serialized tuple
	 * @param colIndex				The index of the desired column
	 * @return						The offset into the <code>serialized</code> array of the start of the field
	 */
	public final int getFieldOffset(byte[] serialized, boolean onlyKey, int tupleOffset, int tupleLength, int colIndex) {
		// Assumes that column is not null or a labeled null
		int fieldOffset, fieldLength = this._columnLengths[colIndex];
		if (onlyKey) {
			fieldOffset = this.keyFieldOffsets[colIndex];
			if (fieldOffset < 0) {
				// not in key tuple
				return -1;
			}
		} else {
			fieldOffset = this.fieldOffsets[colIndex];
		}
		if (fieldLength < 0) {
			// Variable length field
			fieldOffset = IntType.getValFromBytes(serialized, fieldOffset + tupleOffset);
		}
		return tupleOffset + fieldOffset;
	}

	/**
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param tupleOffset			The offset in the <code>serialized</code> array of the start of the tuple
	 * @param tupleLength			The length of the serialized tuple
	 * @param colIndex				The index of the desired column
	 * @return						The length in the <code>serialized</code> array of the field
	 */
	public final int getFieldLength(byte[] serialized, boolean onlyKey, int tupleOffset, int tupleLength, int colIndex) {
		int fieldLength = this._columnLengths[colIndex];
		if (fieldLength >= 0) {
			// Fixed length field
			return fieldLength;
		}
		int fieldOffset;
		if (onlyKey) {
			fieldOffset = this.keyFieldOffsets[colIndex];
			if (fieldOffset < 0) {
				// not in key tuple
				return -1;
			}
		} else {
			fieldOffset = this.fieldOffsets[colIndex];
		}

		// Variable length field
		fieldOffset = IntType.getValFromBytes(serialized, fieldOffset + tupleOffset);
		int nextFieldOffsetOffset = onlyKey ? this.keyNextVariableFieldOffsetOffset[colIndex] : this.nextVariableFieldOffsetOffset[colIndex];
		int nextFieldOffset;
		if (nextFieldOffsetOffset == Integer.MIN_VALUE) {
			// Last variable field in tuple
			nextFieldOffset = tupleLength;
		} else {
			nextFieldOffset = IntType.getValFromBytes(serialized, tupleOffset + nextFieldOffsetOffset);
		}
		return nextFieldOffset - fieldOffset;
	}

	/**
	 * Given a serialized representation of a tuple, get the value from one of its columns
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param tupleLength			The length of the serialized tuple
	 * @param index					The index of the desired column
	 * @return						The value for that column, or <code>null</code> if that column is either a
	 * 								labeled null or null
	 */
	final public Object get(byte[] serialized, boolean onlyKey, int offset, int tupleLength, int index) {
		if (isNull(serialized, onlyKey, offset, index)) {
			return null;
		}

		int fieldOffset = this.getFieldOffset(serialized, onlyKey, offset, tupleLength, index);
		int fieldLength = this.getFieldLength(serialized, onlyKey, offset, tupleLength, index);
		return this._columnTypes[index].fromBytes(serialized, fieldOffset, fieldLength);
	}

	/**
	 * Given a serialized representation of a tuple, get the hash code for one of its columns
	 * 
	 * @param serialized			The array containing the serialized representation of a tuple
	 * @param onlyKey				<code>true</code> if the tuple is a key tuple, <code>false</code> if it is a full tuple
	 * @param offset				The offset in the <code>serialized</code> array of the start of the tuple
	 * @param tupleLength			The length of the serialized tuple
	 * @param index					The index of the desired column
	 * @return						The hash code for that column
	 */
	final public int getColHashCode(byte[] serialized, boolean onlyKey, int offset, int tupleLength, int index) {
		if (isRegularNull(serialized, onlyKey, offset, index)) {
			return 0;
		}
		if (isLabeledNull(serialized, onlyKey, offset, index)) {
			return getLabeledNull(serialized, onlyKey, offset, tupleLength, index);
		}

		final int fieldOffset = this.getFieldOffset(serialized, onlyKey, offset, tupleLength, index);
		final int fieldLength = this.getFieldLength(serialized, onlyKey, offset, tupleLength, index);
		return this._columnTypes[index].getHashCodeFromBytes(serialized, fieldOffset, fieldLength);
	}

	final int getHashCode(boolean first, int cols[], byte[] tuple, boolean onlyKey, int offset, int length) {
		int hashval = 0;
		int multPos = 0;
		int[] mults = first ? AbstractTuple.hashMultipliers1 : AbstractTuple.hashMultipliers2;

		if (cols == null) {
			cols = allColsArray;
		}
		final int numCols = cols.length;
		for (int i = 0; i < numCols; ++i) {
			int col = cols[i];
			int thisColCode = getColHashCode(tuple, onlyKey, offset, length, col);
			if (multPos >= mults.length) {
				hashval = thisColCode + 37 * hashval;
			} else {
				hashval += thisColCode * mults[multPos];
				++multPos;
			}
		}
		return hashval;
	}

	public final int getHashCode(int cols[], byte[] tuple, boolean onlyKey, int offset, int length) {
		return getHashCode(true, cols, tuple, onlyKey, offset, length);
	}
	
	/**
	 * Return a unique series of bytes derived from the specified columns (i.e. all tuples with the
	 * same values for these column will produce the same sequence of bytes
	 * 
	 * @param cols				The columns
	 * @param tuple				A byte array containing the serialized representation of the tuple
	 * @param onlyKey			If the tuple is a key tuple or a full tuple
	 * @param offset			The offset of the start of the tuple into the byte array
	 * @param length			The length of the tuple on the byte array
	 * @return					The byte array 
	 */
	public final byte[] getUniqueBytesForColumns(int[] cols, byte[] tuple, boolean onlyKey, int offset, int length) {
		int neededLength = 0;
		for (int col : cols) {
			if (this.isRegularNull(tuple, onlyKey, offset, col)) {
				neededLength += IntType.bytesPerInt;
			} else if (this.isLabeledNull(tuple, onlyKey, offset, col)) {
				neededLength += 2 * IntType.bytesPerInt;
			} else {
				int fieldLength = this.getFieldLength(tuple, onlyKey, offset, length, col);
				neededLength += IntType.bytesPerInt + fieldLength;
			}
		}
		byte[] retval = new byte[neededLength];
		int pos = 0;
		for (int col : cols) {
			if (this.isRegularNull(tuple, onlyKey, offset, col)) {
				IntType.putBytes(-1, retval, pos);
				pos += IntType.bytesPerInt;
			} else if (this.isLabeledNull(tuple, onlyKey, offset, col)) {
				IntType.putBytes(-2, retval, pos);
				pos += IntType.bytesPerInt;
				IntType.putBytes(this.getLabeledNull(tuple, onlyKey, offset, length, col), retval, pos);
				pos += IntType.bytesPerInt;
			} else {
				int fieldOffset = this.getFieldOffset(tuple, onlyKey, offset, length, col);
				int fieldLength = this.getFieldLength(tuple, onlyKey, offset, length, col);
				IntType.putBytes(fieldLength, retval, pos);
				pos += IntType.bytesPerInt;
				
				for (int i = 0; i < fieldLength; ++i) {
					retval[pos++] = tuple[fieldOffset + i];
				}
			}
		}
		return retval;
	}

	final public boolean equalOnColumn(byte[] serialized1, boolean onlyKey1, int offset1, int length1, byte[] serialized2, boolean onlyKey2, int offset2, int length2, int colIndex) {
		return equalOnColumns(serialized1, onlyKey1, offset1, length1, serialized2, onlyKey2, offset2, length2, colIndex, colIndex);
	}

	final public boolean equalOnColumns(byte[] serialized1, boolean onlyKey1, int offset1, int length1, byte[] serialized2, boolean onlyKey2, int offset2, int length2, int colIndex1, int colIndex2) {
		if (isRegularNull(serialized1, onlyKey1, offset1, colIndex1)) {
			return isRegularNull(serialized2, onlyKey2, offset2, colIndex2);
		} else if (isRegularNull(serialized2, onlyKey2, offset2, colIndex2)) {
			return false;
		}
		if (isLabeledNull(serialized1, onlyKey1, offset1, colIndex1)) {
			return (isLabeledNull(serialized2, onlyKey2, offset2, colIndex2) &&
					getLabeledNull(serialized1, onlyKey1, offset1, length1, colIndex1) == getLabeledNull(serialized2, onlyKey2, offset2, length2, colIndex2));
		} else if (isLabeledNull(serialized2, onlyKey2, offset2, colIndex2)) {
			return false;
		}

		// Field is present and non-null in both tuples
		final int fieldOffset1 = this.getFieldOffset(serialized1, onlyKey1, offset1, length1, colIndex1);
		final int fieldLength1 = this.getFieldLength(serialized1, onlyKey1, offset1, length1, colIndex1);
		final int fieldOffset2 = this.getFieldOffset(serialized2, onlyKey2, offset2, length2, colIndex2);
		final int fieldLength2 = this.getFieldLength(serialized2, onlyKey2, offset2, length2, colIndex2);

		final Type t = this._columnTypes[colIndex1];
		if (t.canCompareSerialized()) {
			if (fieldLength1 != fieldLength2) {
				return false;
			}
			for (int i = 0; i < fieldLength1; ++i) {
				if (serialized1[fieldOffset1 + i] != serialized2[fieldOffset2 + i]) {
					return false;
				}
			}
			return true;
		} else {
			Object o1 = t.fromBytes(serialized1, fieldOffset1, fieldLength1);
			Object o2 = t.fromBytes(serialized2, fieldOffset2, fieldLength2);
			try {
				return (t.compareTwo(o1, o2) == 0);
			} catch (CompareMismatch e) {
				throw new IllegalStateException("Should not get a CompareMismatch when comparing two tuples of the same schema", e);
			}
		}
	}

	private static class WriteableByteArrayCreator implements WriteableByteArray {
		byte[] array;

		@Override
		public int getWriteableByteArrayOffset(int length, boolean writeLength) {
			array = new byte[length];
			return 0;
		}

		@Override
		public byte[] getWriteableByteArray() {
			return array;
		}

	}

	byte[] createTupleFromConstants(Object[] constants, boolean keyTuple) throws ValueMismatchException {
		WriteableByteArrayCreator dest = new WriteableByteArrayCreator();
		new RelationMapping(this, constants).createTuple(dest, keyTuple, null, -1, -1, false);
		return dest.array;
	}


	public static class RelationMapping implements Serializable {
		private static final long serialVersionUID = 1L;
		private final AbstractRelation sourceRel1, sourceRel2, destRel;
		private final int[] constantLabels;
		private final boolean[] constantLabelUsed;
		private final FieldMapping[] fms;
		private final byte[][] serializedConstants;

		public boolean validForInputSchema(AbstractRelation ar) {
			return sourceRel1 != null && ar.quickEquals(sourceRel1) && sourceRel2 == null;
		}

		public boolean validForInputSchemas(AbstractRelation ar1, AbstractRelation ar2) {
			return sourceRel1 != null && sourceRel2 != null && sourceRel1.quickEquals(ar1) && sourceRel2.quickEquals(ar2);
		}

		public boolean validForInputTuple(AbstractImmutableTuple<?> t) {
			return sourceRel1 != null  && sourceRel2 == null && t.getSchema().quickEquals(sourceRel1);
		}

		public boolean validForInputTuples(AbstractImmutableTuple<?> t1, AbstractImmutableTuple<?> t2) {
			return sourceRel1 != null && sourceRel2 != null && t1.getSchema().quickEquals(sourceRel1) && t2.getSchema().quickEquals(sourceRel2);
		}

		public boolean validForOutputSchema(AbstractRelation ar) {
			return ar.quickEquals(destRel);
		}

		public RelationMapping switchInputs() {
			return new RelationMapping(this);
		}

		private RelationMapping(RelationMapping rm) {
			if (rm.sourceRel1 == null || rm.sourceRel2 == null) {
				throw new IllegalArgumentException("Can only create switch inputs on a relation mapping with two inputs");
			}
			this.sourceRel1 = rm.sourceRel2;
			this.sourceRel2 = rm.sourceRel1;
			this.destRel = rm.destRel;
			this.constantLabels = rm.constantLabels;
			this.constantLabelUsed = rm.constantLabelUsed;
			this.serializedConstants = rm.serializedConstants;
			final int fmsLength = rm.fms.length;
			this.fms = new FieldMapping[fmsLength];
			for (int i = 0; i < fmsLength; ++i) {
				this.fms[i] = rm.fms[i].flip();
			}
		}

		private RelationMapping(AbstractRelation rel) {
			sourceRel1 = rel;
			sourceRel2 = null;
			destRel = rel;
			serializedConstants = null;
			fms = new FieldMapping[rel._columnLengths.length];
			for (int i = 0; i < fms.length; ++i) {
				fms[i] = FieldMapping.fromTuple(i);
			}
			constantLabels = null;
			constantLabelUsed = null;
		}

		public RelationMapping(AbstractRelation rel, int[] retainCols) throws ValueMismatchException {
			sourceRel1 = rel;
			sourceRel2 = null;
			destRel = rel;
			serializedConstants = new byte[][] { null};
			int[] sorted = new int[retainCols.length];
			for (int i = 0; i < sorted.length; ++i) {
				sorted[i] = retainCols[i];
			}
			Arrays.sort(sorted);
			fms = new FieldMapping[rel._columnLengths.length];
			int pos = 0;
			FieldMapping nullMapping = FieldMapping.fromConstant(0);
			for (int i = 0; i < fms.length; ++i) {
				if (pos < sorted.length && i == sorted[pos]) {
					++pos;
					fms[i] = FieldMapping.fromTuple(i);
				} else {
					fms[i] = nullMapping;
					if (! rel._columnTypes[i].isNullable()) {
						throw new ValueMismatchException(rel, i);
					}
				}
			}
			constantLabels = null;
			constantLabelUsed = null;
		}

		private RelationMapping(AbstractRelation destRel, Object[] constantFields) throws ValueMismatchException {
			this(null, null, destRel, constantFields, convert(constantFields.length));
		}

		private static FieldMapping[] convert(int numConstants) {
			FieldMapping[] retval = new FieldMapping[numConstants];
			for (int i = 0; i < numConstants; ++i) {
				retval[i] = FieldMapping.fromConstant(i);
			}
			return retval;
		}

		public RelationMapping(AbstractRelation sourceRel, AbstractRelation destRel, Object[] constantFields, FieldSource[] fs) throws ValueMismatchException {
			this(sourceRel, null, destRel, constantFields, convert(fs,destRel._columnTypes.length));
		}

		private static FieldMapping[] convert(FieldSource[] fs, int neededLength) {
			FieldMapping[] retval = new FieldMapping[neededLength];
			if (fs == null) {
				for (int i = 0; i < retval.length; ++i) {
					retval[i] = FieldMapping.fromConstant(i);
				}
			} else {
				for (int i = 0; i < retval.length; ++i) {
					if (fs[i].fromTuple) {
						retval[i] = FieldMapping.fromTuple(fs[i].pos);
					} else {
						retval[i] = FieldMapping.fromConstant(fs[i].pos);
					}
				}
			}
			return retval;
		}

		public RelationMapping(AbstractRelation sourceRel1, AbstractRelation sourceRel2, AbstractRelation destRel, JoinFieldSource jfs[]) throws ValueMismatchException {
			this(sourceRel1, sourceRel2, destRel, null, convert(jfs));
		}

		private static FieldMapping[] convert(JoinFieldSource jfs[]) {
			FieldMapping[] retval = new FieldMapping[jfs.length];
			for (int i = 0; i < retval.length; ++i) {
				if (jfs[i].fromFirstTuple) {
					retval[i] = FieldMapping.fromFirst(jfs[i].pos);
				} else {
					retval[i] = FieldMapping.fromSecond(jfs[i].pos);
				}
			}
			return retval;
		}

		private RelationMapping(AbstractRelation sourceRel1, AbstractRelation sourceRel2, AbstractRelation destRel,
				Object[] constantFields, FieldMapping[] fms) throws ValueMismatchException {
			this.sourceRel1 = sourceRel1;
			this.sourceRel2 = sourceRel2;
			this.destRel = destRel;
			if (constantFields == null) {
				serializedConstants = null;
				constantLabelUsed = null;
				constantLabels = null;
			} else {
				serializedConstants = new byte[constantFields.length][];
				constantLabelUsed = new boolean[constantFields.length];
				constantLabels = new int[constantFields.length];
			}
			this.fms = fms;

			if (fms.length != destRel._columnLengths.length) {
				throw new IllegalArgumentException("Schema " + destRel._name + " has " + destRel._columnLengths.length + " columns but relation mapping only supplies " + fms.length + " columns");
			}


			// Typecheck fields
			for (int i = 0; i < fms.length; ++i) {
				Type destType = this.destRel._columnTypes[i];
				if (fms[i].fieldOrigin.fromTuple) {
					AbstractRelation sourceRel = fms[i].fieldOrigin == FieldOrigin.FIRST ? sourceRel1 : sourceRel2;
					if (fms[i].pos >= sourceRel._columnLengths.length) {
						throw new IllegalArgumentException(" Schema " + sourceRel._name + " has " + sourceRel._columnLengths.length + " columns but column " + fms[i].pos + " is referred to by the relation mapping");
					}
					Type fromType = sourceRel._columnTypes[fms[i].pos];
					if (! fromType.canPutInto(destType)) {
						throw new ValueMismatchException(fromType, destRel, i);
					}
				} else if (constantFields != null) {
					if (fms[i].pos >= constantFields.length) {
						throw new IllegalArgumentException("Constant field #" + fms[i].pos + " is not present in relation mapping");
					}
					Object o = constantFields[fms[i].pos];
					if (o instanceof LabeledNull) {
						if (! destType.isLabeledNullable()) {
							throw new ValueMismatchException(((LabeledNull) o).getLabel(), destRel, i);
						}
						constantLabelUsed[fms[i].pos] = true;
						constantLabels[fms[i].pos] = ((LabeledNull) o).getLabel();
					} else if (o == null) {
						if (! destType.isNullable()) {
							throw new ValueMismatchException(destRel, i);
						}
					} else if (destType.isValidForColumn(o)) {
						serializedConstants[fms[i].pos] = destType.getBytes(constantFields[fms[i].pos]);						
					} else {
						throw new ValueMismatchException(o, destRel, i);
					}
					if (o != null && (! (o instanceof LabeledNull))) {
						if (! destType.isValidForColumn(o)) {
							throw new ValueMismatchException(o, destType);
						}
					}
				}
			}
		}

		public byte[] createTuple(boolean createKeyTuple, byte[] sourceTupleData, int offset, int length, boolean onlyKey) {
			WriteableByteArrayCreator dest = new WriteableByteArrayCreator();
			createTuple(dest, createKeyTuple, sourceTupleData, offset, length, onlyKey);
			return dest.array;
		}

		public byte[] createTuple(boolean createKeyTuple, byte[] sourceTupleData1, int offset1, int length1, boolean onlyKey1,
				byte[] sourceTupleData2, int offset2, int length2, final boolean onlyKey2) {
			WriteableByteArrayCreator dest = new WriteableByteArrayCreator();
			createTuple(dest, createKeyTuple, sourceTupleData1, offset1, length1, onlyKey1, sourceTupleData2, offset2, length2, onlyKey2, null);
			return dest.array;
		}
		
		public void createTuple(WriteableByteArray dest, boolean createKeyTuple, byte[] sourceTupleData, int offset, int length, boolean onlyKey) {
			createTuple(dest, createKeyTuple, sourceTupleData, offset, length, onlyKey, null, Integer.MIN_VALUE, Integer.MIN_VALUE, false, null);
		}
		
		public void createTuple(WriteableByteArray dest, boolean keyTuple, byte[] sourceTupleData1, final int offset1,
				final int length1, final boolean onlyKey1, final byte[] sourceTupleData2, final int offset2, final int length2,
				final boolean onlyKey2, byte[][] serializedFields) {

			if (sourceTupleData2 == null && sourceRel2 != null) {
				throw new IllegalArgumentException("Must supply two tuples to a relation mapping with two input tuples");
			} else if (sourceTupleData2 != null && sourceRel2 == null) {
				throw new IllegalArgumentException("Must supply one tuple to a relation mapping with one input tuple");
			}

			int tupleLength;
			int[] nullFlags, labeledNullFlags, offsets, cols;
			int variableFieldPosition;
			if (keyTuple) {
				tupleLength = destRel.keyTupleFixedLength;
				cols = destRel.keyColsArray;
				nullFlags = destRel.keyTupleNullFlags;
				labeledNullFlags = destRel.keyTupleLabeledNullFlags;
				variableFieldPosition = destRel.keyTupleFixedLength;
				offsets = destRel.keyFieldOffsets;
			} else {
				tupleLength = destRel.fullTupleFixedLength;
				cols = destRel.allColsArray;
				nullFlags = destRel.nullFlags;
				labeledNullFlags = destRel.labeledNullFlags;
				variableFieldPosition = destRel.fullTupleFixedLength;
				offsets = destRel.fieldOffsets;
			}

			// TODO: check lengths of serialized fields?
			byte[][] serializedConstants = serializedFields == null ? this.serializedConstants : serializedFields;
			
			for (int col : cols) {
				if (destRel._columnTypes[col].bytesLength() < 0) {
					if (fms[col].fieldOrigin.fromTuple) {
						byte[] relevantTuple;
						int relevantTupleOffset, relevantTupleLength;
						AbstractRelation sourceRel;
						boolean relevantTupleOnlyKey;
						if (fms[col].fieldOrigin == FieldOrigin.FIRST) {
							relevantTuple = sourceTupleData1;
							relevantTupleOffset = offset1;
							relevantTupleLength = length1;
							sourceRel = sourceRel1;
							relevantTupleOnlyKey = onlyKey1;
						} else {
							relevantTuple = sourceTupleData2;
							relevantTupleOffset = offset2;
							relevantTupleLength = length2;
							sourceRel = sourceRel2;
							relevantTupleOnlyKey = onlyKey2;
						}
						if (sourceRel.isLabeledNull(relevantTuple, relevantTupleOnlyKey, relevantTupleOffset, fms[col].pos)) {
							tupleLength += IntType.bytesPerInt;
						} else if (! sourceRel.isRegularNull(relevantTuple, relevantTupleOnlyKey, relevantTupleOffset, fms[col].pos)) {
							tupleLength += sourceRel.getFieldLength(relevantTuple, relevantTupleOnlyKey, relevantTupleOffset, relevantTupleLength, fms[col].pos);
						}
					} else if (serializedConstants[fms[col].pos] != null) {
						tupleLength += serializedConstants[fms[col].pos].length;
					} else if (constantLabelUsed != null && constantLabelUsed[fms[col].pos]) {
						tupleLength += IntType.bytesPerInt;
					}
				}
			}

			final int tupleOffset = dest.getWriteableByteArrayOffset(tupleLength, true);
			final byte[] tupleDest = dest.getWriteableByteArray();

			COL: for (int col : cols) {
				int fieldOffset = offsets[col];
				int fieldLength = destRel._columnLengths[col];
				final FieldMapping fm = fms[col];
				int labeledNull = -1;
				boolean thisColLabeledNull = false;
				if (labeledNullFlags[col] >= 0) {
					if (fm.fieldOrigin == FieldOrigin.FIRST) {
						if (sourceRel1.isLabeledNull(sourceTupleData1, onlyKey1, offset1, fm.pos)) {
							labeledNull = sourceRel1.getLabeledNull(sourceTupleData1, onlyKey1, offset1, length1, fm.pos);
							thisColLabeledNull = true;
						}
					} else if (fm.fieldOrigin == FieldOrigin.SECOND) {
						if (sourceRel2.isLabeledNull(sourceTupleData2, onlyKey2, offset2, fm.pos)) {
							labeledNull = sourceRel2.getLabeledNull(sourceTupleData2, onlyKey2, offset2, length2, fm.pos);
							thisColLabeledNull = true;
						}
					} else if (constantLabelUsed != null) {
						if (thisColLabeledNull = constantLabelUsed[fm.pos]) {
							labeledNull = constantLabels[fm.pos];
							thisColLabeledNull = true;
						}
					}
					if (thisColLabeledNull) {
						// Set the labeled null flag, write the value of the labeled null at the start of the fixed length section
						// and make sure the rest of the fixed length section is all 0s
						BitSet.setField(labeledNullFlags[col], tupleDest, tupleOffset);
					}
				} else {
					thisColLabeledNull = false;
				}
				final boolean isRegularNull;
				if ((! thisColLabeledNull) && nullFlags[col] >= 0) {
					if (fm.fieldOrigin == FieldOrigin.FIRST) {
						isRegularNull = sourceRel1.isRegularNull(sourceTupleData1, onlyKey1, offset1, fm.pos);
					} else if (fm.fieldOrigin == FieldOrigin.SECOND) {
						isRegularNull = sourceRel2.isRegularNull(sourceTupleData2, onlyKey2, offset2, fm.pos);
					} else {
						isRegularNull = serializedConstants[fm.pos] == null;
					}
					if (isRegularNull) {
						BitSet.setField(nullFlags[col], tupleDest, tupleOffset);
					}
				} else {
					isRegularNull = false;
				}
				final boolean inVariableSection = destRel._columnTypes[col].bytesLength() < 0;

				final byte[] fieldToWrite;
				final int fieldToWriteOffset, fieldToWriteLength;
				if (isRegularNull || thisColLabeledNull) {
					fieldToWrite = null;
					fieldToWriteOffset = -1;
					fieldToWriteLength = -1;
				} else {
					if (fm.fieldOrigin.fromTuple) {
						int relevantTupleLength, relevantTupleOffset;
						AbstractRelation relevantSchema;
						boolean relevantTupleOnlyKey;
						if (fm.fieldOrigin == FieldOrigin.FIRST) {
							fieldToWrite = sourceTupleData1;
							relevantSchema = sourceRel1;
							relevantTupleOffset = offset1;
							relevantTupleLength = length1;
							relevantTupleOnlyKey = onlyKey1;
						} else {
							fieldToWrite = sourceTupleData2;
							relevantSchema = sourceRel2;
							relevantTupleOffset = offset2;
							relevantTupleLength = length2;
							relevantTupleOnlyKey = onlyKey2;
						}
						fieldToWriteOffset = relevantSchema.getFieldOffset(fieldToWrite, relevantTupleOnlyKey, relevantTupleOffset, relevantTupleLength, fm.pos);
						if (fieldToWriteOffset < 0) {
							if (nullFlags[col] >= 0) {
								BitSet.setField(nullFlags[col], tupleDest, tupleOffset);
								fieldToWriteLength = -1;
							} else {
								throw new IllegalArgumentException("Field " + this.sourceRel1.getColName(col) + " is not present is input tuple and is not nullable in output tuple");
							}
						} else {
							fieldToWriteLength = relevantSchema.getFieldLength(fieldToWrite, relevantTupleOnlyKey, relevantTupleOffset, relevantTupleLength, fm.pos);
						}
					} else {
						if (serializedConstants[fm.pos] == null) {
							throw new IllegalArgumentException("Field " + fm.pos + " is null in the constant values of the relation mapping");
						}
						fieldToWrite = serializedConstants[fm.pos];
						fieldToWriteOffset = 0;
						fieldToWriteLength = fieldToWrite.length;
					}
				}

				final int writePos;
				if (inVariableSection) {
					if (isRegularNull) {
						// Write offset of start of next non-null variable-length field (which may be end of tuple)
						IntType.putBytes(variableFieldPosition, tupleDest, tupleOffset + fieldOffset);
						continue COL;
					} else if (thisColLabeledNull) {
						IntType.putBytes(variableFieldPosition, tupleDest, tupleOffset + fieldOffset);
						writePos = tupleOffset + variableFieldPosition;
						variableFieldPosition += IntType.bytesPerInt;
						IntType.putBytes(labeledNull, tupleDest, writePos);
						continue COL;
					} else {
						// Update the offset into the variable length data section
						IntType.putBytes(variableFieldPosition, tupleDest, tupleOffset + fieldOffset);
						writePos = tupleOffset + variableFieldPosition;
						variableFieldPosition += fieldToWriteLength;
					}
				} else {
					if (isRegularNull) {
						// Set fixed length data to zero
						for (int pos = 0; pos < fieldLength; ++pos) {
							tupleDest[tupleOffset + fieldOffset + pos] = 0;
						}
						continue COL;
					} else if (thisColLabeledNull) {
						// Write label followed by zeros in fixed length section 
						IntType.putBytes(labeledNull, tupleDest, tupleOffset + fieldOffset);
						for (int pos = IntType.bytesPerInt; pos < fieldLength; ++pos) {
							tupleDest[tupleOffset + fieldOffset + pos] = 0;
						}
						continue;
					} else {
						writePos = tupleOffset + fieldOffset;
					}
				}
				// Write the actual field data
				for (int i = 0; i < fieldToWriteLength; ++i) {
					tupleDest[writePos + i] = fieldToWrite[fieldToWriteOffset + i];
				}
			}

			if (variableFieldPosition != tupleLength) {
				throw new IllegalStateException("Error when generating tuple, expected and actual tuple lengths don't match, it's a bug");
			}
		}
	}

	private enum FieldOrigin { FIRST(true), SECOND(true), CONSTANT(false);
	final boolean fromTuple;
	FieldOrigin(boolean fromTuple) {
		this.fromTuple = fromTuple;
	}

	FieldOrigin flip() {
		if (this == FIRST) {
			return SECOND;
		} else if (this == SECOND) {
			return FIRST;
		} else {
			return CONSTANT;
		}
	}
	};

	private static class FieldMapping implements Serializable {
		private static final long serialVersionUID = 1L;
		final int pos;
		final FieldOrigin fieldOrigin;

		private FieldMapping(int pos, FieldOrigin fieldOrigin) {
			this.pos = pos;
			this.fieldOrigin = fieldOrigin;
		}

		static FieldMapping fromTuple(int pos) {
			return new FieldMapping(pos, FieldOrigin.FIRST);
		}

		static FieldMapping fromFirst(int pos) {
			return new FieldMapping(pos, FieldOrigin.FIRST);
		}

		static FieldMapping fromSecond(int pos) {
			return new FieldMapping(pos, FieldOrigin.SECOND);
		}

		static FieldMapping fromConstant(int pos) {
			return new FieldMapping(pos, FieldOrigin.CONSTANT);
		}
		
		FieldMapping flip() {
			return new FieldMapping(pos, fieldOrigin.flip());
		}
	}

	public static class FieldSource {
		public final int pos;
		public final boolean fromTuple;
		public FieldSource(int pos, boolean fromTuple) {
			this.pos = pos;
			this.fromTuple = fromTuple;
		}
	}

	public static class JoinFieldSource {
		public final int pos;
		public final boolean fromFirstTuple;
		public JoinFieldSource(int pos, boolean fromFirstTuple) {
			this.pos = pos;
			this.fromFirstTuple = fromFirstTuple;
		}
	}

	public static class JoinComparator {
		public final AbstractRelation rel1, rel2;
		private final int[] rel1cols, rel2cols;

		public JoinComparator(AbstractRelation rel1, AbstractRelation rel2, int[] rel1cols, int[] rel2cols) throws ValueMismatchException {
			this.rel1 = rel1;
			this.rel2 = rel2;
			if (rel1cols.length != rel2cols.length) {
				throw new IllegalArgumentException("Join column arrays must have the same length");
			}
			this.rel1cols = new int[rel1cols.length];
			this.rel2cols = new int[rel2cols.length];

			for (int i = 0; i < rel1cols.length; ++i) {
				this.rel1cols[i] = rel1cols[i];
				this.rel2cols[i] = rel2cols[i];

				if (rel1._columnTypes[rel1cols[i]].getClass() != rel2._columnTypes[rel2cols[i]].getClass()) {
					throw new ValueMismatchException(rel1._columnTypes[rel1cols[i]], rel2._columnTypes[rel2cols[i]]);
				}
			}
		}
		
		private JoinComparator(int[] rel1cols, int[] rel2cols,
				AbstractRelation rel1, AbstractRelation rel2) {
			this.rel1cols = rel2cols;
			this.rel2cols = rel1cols;
			this.rel1 = rel2;
			this.rel2 = rel1;
		}

		public JoinComparator swapInputs() {
			return new JoinComparator(rel1cols, rel2cols, rel1, rel2);
		}

		public boolean validForInputSchemas(AbstractRelation rel1, AbstractRelation rel2) {
			return rel1.quickEquals(this.rel1) && rel2.quickEquals(this.rel2);
		}

		public boolean joins(byte[] tuple1, int offset1, int length1, boolean onlyKey1, byte[] tuple2, int offset2,
				int length2, boolean onlyKey2) {
			final int numJoinCols = rel1cols.length;
			try {
				for (int i = 0; i < numJoinCols; ++i) {
					final int col1 = rel1cols[i];
					final int col2 = rel2cols[i];
					if (rel1.isRegularNull(tuple1, onlyKey1, offset1, col1)) {
						if (! rel2.isRegularNull(tuple2, onlyKey2, offset2, col2)) {
							return false;
						}
					} else if (rel2.isRegularNull(tuple2, onlyKey2, offset2, col2)) {
						return false;
					} else if (rel1.isLabeledNull(tuple1, onlyKey1, offset1, col1)) {
						if (rel2.isLabeledNull(tuple2, onlyKey2, offset2, col2)) {
							if (rel1.getLabeledNull(tuple1, onlyKey1, offset1, length1, col1) !=
								rel2.getLabeledNull(tuple2, onlyKey2, offset2, length2, col2)) {
								return false;
							}
						} else {
							return false;
						}
					} else if (rel2.isLabeledNull(tuple2, onlyKey2, offset2, col2)) {
						return false;
					} else {
						final int fieldOffset1 = rel1.getFieldOffset(tuple1, onlyKey1, offset1, length1, col1);
						final int fieldLength1 = rel1.getFieldLength(tuple1, onlyKey1, offset1, length1, col1);
						final int fieldOffset2 = rel2.getFieldOffset(tuple2, onlyKey2, offset2, length2, col2);
						final int fieldLength2 = rel2.getFieldLength(tuple2, onlyKey2, offset2, length2, col2);
						Type t = rel1._columnTypes[col1];
						if (t.canCompareSerialized()) {
							if (fieldLength1 != fieldLength2) {
								return false;
							}
							for (int j = 0; j < fieldLength1; ++j) {
								if (tuple1[fieldOffset1 + j] != tuple2[fieldOffset2 + j]) {
									return false;
								}
							}
						} else {
							Object o1 = t.fromBytes(tuple1, fieldOffset1, fieldLength1);
							Object o2 = t.fromBytes(tuple2, fieldOffset2, fieldLength2);
							if (t.compareTwo(o1, o2) != 0) {
								return false;
							}
						}
					}
				}
			} catch (CompareMismatch cm) {
				throw new IllegalStateException("Should not get a CompareMismatch after typechecking", cm);
			}
			return true;
		}
	}
	
	public boolean equals(byte[] data, boolean onlyKey, int offset, int length, int col, byte[] constant) {
		if (this.isNull(data, onlyKey, offset, col)) {
			return false;
		}
		int colLength = this.getFieldLength(data, onlyKey, offset, length, col);
		if (colLength != constant.length) {
			return false;
		}
		int colOffset = this.getFieldOffset(data, onlyKey, offset, length, col);
		for (int i = 0; i < colLength; ++i) {
			if (data[colOffset + i] != constant[i]) {
				return false;
			}
		}
		return true;
	}
}
