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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.sql.dtp.ValueExpressionColumnConstant;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;



/**
 * A class to represent a relation that exists inside a schema
 * for a collection of relations.
 * 
 * @author netaylor
 *
 */
public class Relation extends AbstractRelation {

	public static final int NO_ID = -1;
	private static final long serialVersionUID = 1L;

	private /*final */ Schema _schema;
	private /*final */ int _relationID = NO_ID;
	private boolean _hasLNs = true;

	public static final String LOCAL = "_L";
	public static final String REJECT = "_R";

	public static final String INSERT = "_INS";
	public static final String DELETE = "_DEL";

	public static String valueAttrName = "VALUE";

	/**
	 * What mapping tables' names start with.
	 */
	public static final String MAPPING_PREFIX = "M";

	/** Fields of this relation that appear in some skolem term */
	protected List<RelationField> _skolemizedFields = new ArrayList<RelationField> ();
	
	protected List<Boolean> _nullableFields = new ArrayList<Boolean>();

	/** Catalog used for the relation in the source database, might be null */
	protected String _dbCatalog;
	/** Schema used for the relation in the source database, might be null */
	protected String _dbSchema;
	/** Relation physical name in the database. Can be equal to the 
	 * "Orchestra name" (_name) but the Orchestra name has to be 
	 * unique for a given Orchestra schema
	 */
	protected String _dbRelName;

	/** A relation can be virtual or materialized **/
	protected boolean _materialized;

	/** A relation may have local data in name_L relation **/
	protected boolean _hasLocalData;

	/**
	 * Create a new schema 
	 */
	public Relation(Schema schema, int relationID) {
		super();
		_relationID = relationID;
		_schema = schema;
//		if (Config.getTempTables())
//			_dbSchema = SqlStatementGen.sessionSchema;

		if (schema != null)
			_name = schema.getNameForID(relationID);
		
		//_nullableFields = new ArrayList<Boolean>(schema.getRelationSchema(relationID).getNumCols());
		_nullableFields = new ArrayList<Boolean>();
		for (int i = 0; i < getNumCols(); i++)
			_nullableFields.add(i, new Boolean(false));
	}

	public Relation(Schema schema, int relationID, AbstractRelation tab, boolean materialized, boolean hasLocalData) {
		super(tab);
		_relationID = relationID;
		_schema = schema;

		if (tab instanceof Relation) {
			_hasLNs = ((Relation)tab)._hasLNs;
			int siz = ((Relation)tab)._nullableFields.size();
			_nullableFields = new ArrayList<Boolean>(siz);
			for (int i = 0; i < siz; i++)
				_nullableFields.add(((Relation)tab)._nullableFields.get(i));
		} else {
			_nullableFields = new ArrayList<Boolean>(schema.getRelationSchema(relationID).getNumCols());
			for (int i = 0; i < _nullableFields.size(); i++)
				_nullableFields.add(i, new Boolean(false));
		}

		//		if (Config.getTempTables())
//			_dbSchema = SqlStatementGen.sessionSchema;
		
		if (schema != null && _name == null)
			_name = schema.getNameForID(relationID);
		if (relationID != Relation.NO_ID && this._schema != null && 
				!schema.getNameForID(relationID).equals(getName()))
			throw new IllegalStateException("Relation's internal name and schema name differ!  " +
					schema.getNameForID(relationID) + " / " + getName());

		if (tab._pk != null){
			List<String> keyFields = new ArrayList<String>();
			for (RelationField fld : tab._pk.getFields()){
				keyFields.add(fld.getName());
			}
			try{
				_pk = new PrimaryKey(tab._pk.getName(), this, keyFields);
			}catch(UnknownRefFieldException e){
//				Should never happen
				e.printStackTrace();
			}
//			_pk = tab._pk;
		}

		for (RelationIndexUnique idx : tab.getUniqueIndexes())
			_uniqueIndexes.add(idx);
		for (RelationIndexNonUnique idx : tab.getNonUniqueIndexes())
			_nonUniqueIndexes.add(idx);

		_materialized = materialized;
		_hasLocalData = hasLocalData;
		finished = tab.finished;
	}

	public Relation(Schema schema, AbstractRelation tab,
			String nam, String dbName, String dbSchema, boolean materialized, boolean hasLocalData) {
		super(tab);

		_materialized = materialized;
		_hasLocalData = hasLocalData;

		if (tab instanceof Relation) {
			_hasLNs = ((Relation)tab)._hasLNs;
			int siz = ((Relation)tab)._nullableFields.size();
			_nullableFields = new ArrayList<Boolean>(siz);
			for (int i = 0; i < siz; i++)
				_nullableFields.add(((Relation)tab)._nullableFields.get(i));
		} else {
			_nullableFields = new ArrayList<Boolean>(tab.getNumCols());
			for (int i = 0; i < _nullableFields.size(); i++)
				_nullableFields.add(new Boolean(false));
		}
		_name = nam;
		_dbRelName = dbName;
		this._schema = schema;
//		if (Config.getTempTables())
//			_dbSchema = SqlStatementGen.sessionSchema;
//		else
			this._dbSchema = dbSchema;

		try {
			this._relationID = schema.addRelation(this);
		} catch (DuplicateRelationIdException dne) {

		}

		if (tab._pk != null){
			List<String> keyFields = new ArrayList<String>();
			for (RelationField fld : tab._pk.getFields()){
				keyFields.add(fld.getName());
			}
			try{
				_pk = new PrimaryKey(tab._pk.getName(), this, keyFields);
			}catch(UnknownRefFieldException e){
//				Should never happen
				e.printStackTrace();
			}
//			_pk = tab._pk;
		}

		for (RelationIndexUnique idx : tab.getUniqueIndexes())
			_uniqueIndexes.add(idx);
		for (RelationIndexNonUnique idx : tab.getNonUniqueIndexes())
			_nonUniqueIndexes.add(idx);
		finished = tab.finished;
	}

	/**
	 * Creates a new relation
	 * @param dbCatalog Catalog used for the relation in the source database, can be null
	 * @param dbSchema Schema used for the relation in the source database, can be null
	 * @param dbRelName Relation name in the actual database
	 * @param name Relation name
	 * @param description Relation description
	 * @param fields Relation fields
	 */
	public Relation (String dbCatalog, String dbSchema, String dbRelName, String name, 
			String description, boolean materialized, boolean hasLocalData, List<RelationField> fields) 
	{
		super(name, description, fields);
		for(RelationField field : fields)
			field.setRelation(this);
		_nullableFields = new ArrayList<Boolean>(fields.size());
		for (int i = 0; i < _nullableFields.size(); i++)
			_nullableFields.add(new Boolean(false));
		_dbCatalog = dbCatalog;
		_dbSchema = dbSchema;
		_dbRelName = dbRelName;
		_materialized = materialized;
		_hasLocalData = hasLocalData;
	}	

	/**
	 * Creates a new relation
	 * @param dbCatalog Catalog used for the relation in the source database, can be null
	 * @param dbSchema Schema used for the relation in the source database, can be null
	 * @param dbRelName Relation name in the actual database
	 * @param name Relation name
	 * @param description Relation description
	 * @param fields Relation fields
	 * @throws UnknownRefFieldException 
	 */
	public Relation (String dbCatalog, String dbSchema, String dbRelName, String name, 
			String description, boolean materialized, boolean hasLocalData, List<RelationField> fields, 
			String pkName, List<String> pkFieldNames) throws UnknownRefFieldException 
			{
		this (dbCatalog, dbSchema, dbRelName, name, description, materialized, hasLocalData,
				fields, pkName, pkFieldNames, true);
			}
	
	public Relation (String dbCatalog, String dbSchema, String dbRelName, String name, 
			String description, boolean materialized, boolean hasLocalData, List<RelationField> fields, 
			String pkName, List<String> pkFieldNames, boolean finishNow) throws UnknownRefFieldException 
			{
		super (name, description, fields, pkName, pkFieldNames, false);

		_nullableFields = new ArrayList<Boolean>(fields.size());
		for (int i = 0; i < _nullableFields.size(); i++)
			_nullableFields.add(new Boolean(false));
		_dbCatalog = dbCatalog;
		_dbSchema = dbSchema;
		_dbRelName = dbRelName;
		_materialized = materialized;
		_hasLocalData = hasLocalData;
		
		if (finishNow)
			markFinished();
			}	

	/**
	 * Deep copy of the relation
	 * Use the method deepCopy to benefit from polymorphism
	 * @param relation Relation to copy
	 * @roseuid 449AEA650271
	 * @see AbstractRelation#deepCopy()
	 */
	protected Relation(Relation relation) 
	{
		this(relation._schema, relation._relationID, relation, relation.isMaterialized(), relation.hasLocalData());
//		if (Config.getTempTables())
//			_dbSchema = SqlStatementGen.sessionSchema;
		
		_hasLNs = relation._hasLNs;
		int siz = relation._nullableFields.size();
		_nullableFields = new ArrayList<Boolean>(siz);
		for (int i = 0; i < siz; i++)
			_nullableFields.add(relation._nullableFields.get(i));

		_dbCatalog = relation.getDbCatalog();
		_dbSchema = relation.getDbSchema();
		_dbRelName = relation.getDbRelName();
//		_materialized = relation.isMaterialized();

		//this.relationID = relation.relationID;
		//this.schema = relation.schema;
	}   

	protected void setNewID(int relID) {
		_relationID = relID;
	}

	/**
	 * Set the schema / relation ID
	 * 
	 * @param s
	 * @param relID
	 */
	public void setSchema(Schema s, int relID) {
		this._relationID = relID;
		this._schema = s;
	}

	/**
	 * Add a relation to a schema
	 * 
	 * @param s
	 */
	public void addToSchema(Schema s) throws IllegalStateException, DuplicateRelationIdException {
		if (finished) {
			throw new IllegalStateException("Cannot add relation to finished schema");
		} else if (this._schema != null && this._schema != s) {
			throw new IllegalStateException("Cannot move relation from one schema to another");
		}

		this._relationID = s.addRelation(this);
		this._schema = s;
		_name = _schema.getNameForID(_relationID);
	}


	public String getRelationName() throws IllegalStateException {
		if (this._schema != null && NO_ID != _relationID && !_schema.getNameForID(_relationID).equals(getName()) )
			throw new IllegalStateException("Relation's internal name and schema name differ!  " +
					_schema.getNameForID(_relationID) + " / " + getName());
		return getName();//schema.getNameForID(relationID);
	}

	public int getRelationID() throws IllegalStateException {
		if (this._schema == null)
			throw new IllegalStateException("Cannot get relation ID from a relation that has no associated schema!");

		return _relationID;
	}

	public void setRelationID(int id) {
		_relationID = id;
	}

	/**
	 * Get a deep copy of this relation. <BR>
	 * Deep copy won't copy the foreign keys
	 * @return Deep copy
	 * @see AbstractRelation#TableSchema(AbstractRelation)
	 * @see AbstractRelation#deepCopyFks(Schema)
	 */
	/*protected*/
	public synchronized Relation deepCopy ()
	{
		return new Relation(_schema, _relationID, this, _materialized, _hasLocalData);
	}   

	public synchronized Relation deepCopy (String newName, String newDbName, String newDbSchema)
	{
		return new Relation(_schema, this, newName, newDbName, newDbSchema, _materialized, _hasLocalData);

	}

	/**
	 * Complete deep copy
	 * @return
	 */
	public synchronized Relation deepCopyFull()
	{
		Relation rel = deepCopy();
		rel.finished = false;
		//rel.deepCopyFks(this, null);
		rel._hasLNs = _hasLNs;
		
		for (RelationField f : _skolemizedFields)
			rel._skolemizedFields.add(f);
		
		for (Boolean b : _nullableFields)
			rel._nullableFields.add(b);
		
		rel._dbCatalog = _dbCatalog;
		rel._dbSchema = _dbSchema;
		rel._dbRelName = _dbRelName;
		rel._materialized = _materialized;
		rel._hasLocalData = _hasLocalData;
		
		return rel;
	}

	public synchronized void deepCopyFks (AbstractRelation relation, Schema schema)
	{
		for (ForeignKey cst : relation.getForeignKeys())
			_foreignKeys.add(cst);

	}

	public boolean hasLabeledNulls() {
		return _hasLNs;
	}

	public void setLabeledNulls() {
		if (finished) {
			throw new RuntimeException("Attempt to modify a finished schema");
		}
		_hasLNs = true;
	}

	public void setLabeledNulls(boolean b) {
		_hasLNs = b;
	}


	public synchronized void serialize(Document doc, Element schema) {
		// TODO: rewrite to use AbstractRelation.serialize
		schema.setAttribute("name", _name);
		schema.setAttribute("description", _description);
		schema.setAttribute("materialized", _materialized ? "true" : "false");
		schema.setAttribute("hasLocalData", _hasLocalData ? "true" : "false");
		schema.setAttribute("noNulls", hasLabeledNulls() ? "false" : "true");
		Element dbinfo = DomUtils.addChild(doc, schema, "dbinfo");
		dbinfo.setAttribute("catalog", _dbCatalog);
		dbinfo.setAttribute("schema", _dbSchema);
		dbinfo.setAttribute("table", _dbRelName);
		super.serialize(doc, schema);
		
		// Output the set of attributes that require labeled nulls
		if (Config.useCompactNulls()) {
			for (int i = 0; i < getFields().size(); i++) {
				Element field = DomUtils.addChild(doc, schema, "labeledNull");
				if (isNullable(i))
					field.setAttribute("set", "true");
				else
					field.setAttribute("set", "false");
			}
		}
	}

//	public TableSchema (String dbCatalog, String dbSchema, String dbRelName, String name, 
//	String description, boolean materialized, List<ScField> fields,
//	int statNbRows) 
	public static Relation deserialize(Element relElt, boolean builtinFunction) throws XMLParseException, UnknownRefFieldException, UnsupportedTypeException {
		// TODO: rewrite to use AbstractRelation.deserializeAbstractRelation
		String noNulls = relElt.getAttribute("noNulls");

		String name = relElt.getAttribute("name");
		String descr = relElt.getAttribute("description");
		boolean mat = Boolean.parseBoolean(relElt.getAttribute("materialized"));
		boolean hasLocal = Boolean.parseBoolean(relElt.getAttribute("hasLocalData"));
		Element dbinfo = DomUtils.getChildElementByName(relElt, "dbinfo");
		if (dbinfo == null) {
			throw new XMLParseException("Missing dbinfo element under relation element", relElt);
		}
		String dbcatalog = dbinfo.getAttribute("catalog");
		if (dbcatalog.length() == 0) {
			dbcatalog = null;
		}
		String dbschema = dbinfo.getAttribute("schema");
		String dbtable = dbinfo.getAttribute("table");
		ArrayList<RelationField> fields = new ArrayList<RelationField>();
		List<String> keyFields = new ArrayList<String>();
		for (Element field : DomUtils.getChildElementsByName(relElt, "field")) {
			RelationField f = RelationField.deserialize(field);
			fields.add(f);
			if("true".equals(field.getAttribute("key"))){
				keyFields.add(field.getAttribute("name"));
			}
		}

		Element primaryKey = DomUtils.getChildElementByName(relElt, "primaryKey");
		String pkName = null;
		List<String> pkFieldNames = new ArrayList<String>();
		if (primaryKey != null) {
			pkName = primaryKey.getAttribute("name");
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
		}else{
			pkName = "pk";
			pkFieldNames.addAll(keyFields);
		}

//		greg: hack to add edb bit to peer relations
		if(Config.getEdbbits() && !builtinFunction){
			RelationField f = MappingsTranslationMgt.edbBitField();
			fields.add(f);
			pkFieldNames.add(f.getName());
		}

		// Mark as finished IF we aren't in trust-annotation-adding mode
		Relation rel = new Relation(dbcatalog, dbschema, dbtable, name, descr, mat, hasLocal, fields, 
				pkName, pkFieldNames, !Config.addTrustAnnotations());
		
		if (noNulls.equals("true"))
			rel._hasLNs = false;
		else
			rel._hasLNs = true;
		
		rel._nullableFields = new ArrayList<Boolean>(fields.size());
		for (int i = 0; i < fields.size(); i++)
			rel._nullableFields.add(new Boolean(false));
		
		int inx = 0;
		for (Element field : DomUtils.getChildElementsByName(relElt, "labeledNull")) {
			RelationField f = RelationField.deserialize(field);
			fields.add(f);
			if("true".equals(field.getAttribute("set"))){
				rel._nullableFields.set(inx, true);
			}
		}
		
		return rel;
	}

	public boolean isInternalRelation() {
		return getName().endsWith(LOCAL) || getName().endsWith(REJECT);
	}

	public static boolean isInternalRelationName(String fn) {
		return fn.endsWith(LOCAL) || fn.endsWith(REJECT);
	}

	/**
	 * Return <code>true</code> if <code>s</code> follows the naming convention
	 * for mapping tables, <code>false</code> otherwise.
	 * 
	 * @param s that which is being tested.
	 * @return see description.
	 */
	public static boolean isMappingTableName(String s) {
		return s.matches(MAPPING_PREFIX + "\\d+");
	}

	public String getLocalName() {
		return getName() + LOCAL;
	}

	public String getRejectName() {
		return getName() + REJECT;
	}

	public String getLocalInsDbName() {
		return getDbRelName() + LOCAL;
	}

	public String getLocalRejDbName() {
		return getDbRelName() + REJECT;
	}

	/**
	 * Set the skolemized fields for this relation
	 * @param the fields of this relation that appear in some skolem term
	 * @deprecated
	 */
	public synchronized void setSkolemizedFields (List<RelationField> skolemizedFields){
		_skolemizedFields = skolemizedFields;
	}

	/**
	 * @return List of fields of this relation that 
	 * appear in some skolem term 
	 * @deprecated
	 */
	public synchronized List<RelationField> getSkolemizedFields ()
	{
		return Collections.unmodifiableList(_skolemizedFields);
	}

	/**
	 * Get the database catalog for this relation
	 * @return Database catalog 
	 */
	public String getDbCatalog ()
	{
		return _dbCatalog;	   
	}

	/**
	 * Get the database schema for this relation
	 * @return Database schema
	 */
	public String getDbSchema ()
	{
//		if (Config.getTempTables())
//			return SqlStatementGen.sessionSchema;
//		else
			return _dbSchema;
	}

	/** 
	 * Get the database relation name for this relation
	 * (can be different from Orchestra relation name which 
	 * must be unique for a given Orchestra schema) 
	 * @return Database table name
	 */
	public String getDbRelName ()
	{
		return _dbRelName;
	}
	/**
	 * Get the fully qualified id of this relation over the underlying database.<BR>
	 * The id is computed based on the DbCatalog, the DbSchema and the DbName. Thus this id 
	 * is supposed to be unique for a given database.
	 * @return The relation id
	 * @see Schema
	 */  
	public String getFullQualifiedDbId ()
	{
		//	 TODO:LOW Could a schema be defined over multiple dbs? Sounds not likely to occur with orchestra for 
		//	 materialized schemas but what about coming virtual schemas? 
//		if (Config.getTempTables())
//			return getFullQualifiedDbId(getDbCatalog(), SqlStatementGen.sessionSchema, getName());
//		else
			return getFullQualifiedDbId(getDbCatalog(), getDbSchema(), getName());
	}

	public String getFullQualifiedOUProvDbId ()
	{
		//	 TODO:LOW Could a schema be defined over multiple dbs? Sounds not likely to occur with orchestra for 
		//	 materialized schemas but what about coming virtual schemas? 
		return getFullQualifiedDbId(getDbCatalog(), getDbSchema(), "p" + getName());
	}

	/**
	 * Compute a relation fully qualified db name based on the DbCatalog, 
	 * the DbSchema and the DbName
	 * @param catalog DbCatalog for the relation id to compute
	 * @param schema  DbSchema for the relation id to compute
	 * @param name  Relation's name
	 * @return Relation id
	 * @see Schema
	 */
	public static String getFullQualifiedDbId (String catalog, String schema, String name)
	{
		String id="";
//
//		if(Config.isDB2() && Config.getUseTempTables()){
//			id = SqlStatementGen.sessionSchema + "." + name;
//		}else{
			if (catalog != null)
				id = catalog + ".";
			if (schema != null)
				id += schema + ".";
//			else
//			id += "ORCHESTRA" + ".";
			id += name;

//		}

		return id;
	}

	/**
	 * Returns a description of the relation, conforms to the 
	 * flat file format defined in <code>RepositoryDAO</code>
	 * @return Relation description
	 */   
	public String toString ()
	{
		return toString (0);
	}

	/**
	 * Returns a description of the relation, conforms to the 
	 * flat file format defined in <code>RepositoryDAO</code>
	 * Description is indented with <code>nbTabs</code> tabs
	 * @param nbTabs Nb of tabulations
	 * @return Relation description
	 */   
	protected synchronized String toString (int nbTabs)
	{
		StringBuffer buff = new StringBuffer (); 

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("RELATION ");
		buff.append(getName());
		buff.append(" \"" + getDescription() + "\"");
		if (isMaterialized())
			buff.append(" MATERIALIZED");
		else
			buff.append(" VIRTUAL");
		buff.append("\n");

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("\tDBINFO ");
		buff.append((getDbCatalog()!=null?getDbCatalog():"") + ", ");
		buff.append((getDbSchema()!=null?getDbSchema():"") + ", ");
		buff.append(getDbRelName() + "\n");

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("\tFIELDS ");
		boolean firstField = true;
		for (RelationField fld : getFields())
		{
			buff.append ((firstField?"":", ") + fld.toString());
			firstField = false;
		}
		buff.append("\n");


		if (_pk != null)
		{
			for (int i = 0 ; i < nbTabs ; i++)
				buff.append("\t");
			buff.append("\t");		
			buff.append (_pk.toString());
			buff.append("\n");

		}



		for (RelationIndexUnique ux : _uniqueIndexes)
		{
			for (int i = 0 ; i < nbTabs ; i++)
				buff.append("\t");
			buff.append ("\t");
			buff.append (ux.toString() + "\n");
		}

		for (RelationIndexNonUnique ux : _nonUniqueIndexes)
		{
			for (int i = 0 ; i < nbTabs ; i++)
				buff.append("\t");
			buff.append ("\t");
			buff.append (ux.toString() + "\n");
		}

		for (ForeignKey fk : _foreignKeys)
		{
			for (int i = 0 ; i < nbTabs ; i++)
				buff.append("\t");
			buff.append ("\t");
			buff.append (fk.toString() + "\n");
		}		

		return buff.toString();

	}

	public synchronized String toAtomString(AtomType type) {
		// returns a string of form: R(A,B,C,D)
		StringBuffer buf = new StringBuffer();
		//Olivier: fix to work with multiple schemas
		//buf.append(getName());
		buf.append(getFullQualifiedDbId());
		buf.append(Atom.typeToSuffix(type));
		buf.append("(");
		List<RelationField> fields = getFields();
		for (int i = 0; i < fields.size(); i++) {
			if (i != 0) {
				buf.append(",");
			}
			buf.append(fields.get(i).getName());
		}
		buf.append(")");
		return buf.toString();
	}
	
	public synchronized List<String> getFieldsInList() {
		List<String> ret = new ArrayList<String>();
		for (RelationField f : getFields()) {
			ret.add(f.getName());
		}
	
		return ret;
	}

	/**
	 * Is this relation materialized or virtual
	 * @return
	 */
	public boolean isMaterialized ()
	{
		return _materialized;
	}

	/** A relation may have local data in name_L relation **/
	public boolean hasLocalData ()
	{
		return _hasLocalData;
	}
	
	public void setIsNullable(int inx) {
		setIsNullable(inx, true);
	}
	
	public void clearIsNullable(int inx) {
		setIsNullable(inx, false);
	}
	
	public void setIsNullable(int inx, boolean nul) {
		while (_nullableFields.size() <= inx)
			_nullableFields.add(new Boolean(false));
		
		_nullableFields.set(inx, nul);
		
		Type t = getColType(inx); 
		if (t != null)
			t.setLabeledNullable(nul);
	}
	
	public String getColumnsAsString() {
		StringBuffer retval = new StringBuffer("(");
		int size = _columnTypes.length;//_fields.size();
		for (int i = 0; i < size; ++i) {
			String name = getField(i).getName();
			if (isNullable(i))
				name = name + "*";
			retval.append(name);
			if (i != (size - 1)) {
				retval.append(",");
			}
		}
		retval.append(")");
		return retval.toString();
	}

	/**
	 * Can have labeled null field
	 * 
	 * @param inx
	 * @return
	 */
	public boolean isNullable(int inx) {
		if (inx >= _nullableFields.size())
			return false;
		else
			return _nullableFields.get(inx);
	}
	
	/** 
	 * Get next revision for the current relation
	 * _VER1 if now previous revision exists 
	 * @return Database table name
	 */
	public String getNextRevisionName ()
	{
		String relName = _dbRelName;
		String nextRevision = "_VER";
		int intVersionIndex = _dbRelName.indexOf("_VER");
		if (intVersionIndex != -1)
		{
			relName = _dbRelName.substring(0,intVersionIndex);
			String currRevision = _dbRelName.substring(intVersionIndex+4,
					_dbRelName.length());
			nextRevision += (new Integer
					(Integer.parseInt(currRevision)+1)).toString();
			System.out.println("nextRevision-->"+nextRevision);
		}
		else
		{
			System.out.println("First revision");
			nextRevision += "1";
		}
		return relName + nextRevision;
	}

	@Override
	public boolean quickEquals(AbstractRelation ar) {
		Relation r = (Relation) ar;
		return r._relationID == _relationID;
	}
	
	public void deriveLabeledNulls(List<Relation> rels) {
		boolean noNulls = true;
		
		for(Relation rel : rels){
			if(rel.hasLabeledNulls()){
				noNulls = false;
				break;
			}
		}
		if(noNulls){
			setLabeledNulls(false);
		}
	}
	
	public void deriveLabeledNullsFromAtoms(List<Atom> atoms) {
		boolean noNulls = true;
		
		for(Atom a : atoms){
			if(a.getRelation().hasLabeledNulls()){
				noNulls = false;
				break;
			}
		}
		if(noNulls){
			setLabeledNulls(false);
		}
	}
	
	public String getPreferredIndexName(String suffix) {
		final String tableName = getDbRelName();
		
		String ext = suffix;
		if (!suffix.isEmpty())
			ext = "_" + ext;


		final String qualifiedRelName = (getDbSchema() != null ? (getDbSchema() + ".") : "")
				+ tableName;
		
		return qualifiedRelName + ext + "_INDX";
	}

	public String getQualifiedName(String suffix) {
		final String tableName = getDbRelName();

		String ext = suffix;
		if (!suffix.isEmpty())
			ext = "_" + ext;

		final String qualifiedRelName = (getDbSchema() != null ? (getDbSchema() + ".") : "")
				+ tableName;
		
		return qualifiedRelName + ext;
	}
}