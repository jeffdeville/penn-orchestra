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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


public class Schema implements Serializable, AbstractTuple.TupleFactory<Relation,Tuple> {
	private static final long serialVersionUID = 1L;
	private HashMap<String,Integer> relationIndices;
	private HashMap<String,Integer> relationIDs;
	private HashMap<Integer,Integer> IDToIndex;
	private ArrayList<String> relationNames;
	private ArrayList<Relation> relationSchemas;
	private boolean finished;
	private static final Logger logger = LoggerFactory.getLogger(Schema.class);
	
	//	private int relOffset;

	/** Schema id. Should be unique for a given peer, uniqueness not managed in this class */
	private String _schemaId;
	/** Schema description */
	private String _description;

	/** 
	 * List of relations stored in this schema. The id used in the map is the relation name
	 * @see Relation#getRelId()
	 * @see Relation#getRelId(String, String, String)
	 */
	//	protected Map<String,RelationSchema> _relations = new Hashtable<String,RelationSchema>();



	/*
	public Schema(edu.upenn.cis.orchestra.repository.model.Schema schema) throws TableSchema.BadColumnName {
		this();
		for (RelationSchema scr : schema.getRelations()) {
			RelationSchema rs = addRelation(scr.getName());
			for (ScField field : scr.getFields()) {
				OptimizerType se = field.getType();
				rs.addCol(field.getName(), se);
			}
		}
		markFinished();
	}*/

	public Schema(String schemaId) {
		_schemaId = schemaId;
		relationIndices = new HashMap<String,Integer>();
		relationIDs = new HashMap<String,Integer>();
		IDToIndex = new HashMap<Integer,Integer>();
		relationNames = new ArrayList<String>();
		relationSchemas = new ArrayList<Relation>();
		finished = false;
	}

	/**
	 * Creates a new schema
	 * @param schemaId Schema id. Should be unique for a given peer, unicity not managed in this class
	 * @param description Schema description
	 * @roseuid 449AE40F0109
	 */
	public Schema(String schemaId, String description) 
	{
		this(schemaId);
		_description = description;
	}


	/**
	 * Creates a deep copy of a given schema
	 * @param schema Schema to copy
	 */
	public Schema (Schema schema)
	{
		relationIndices = new HashMap<String,Integer>();
		relationNames = new ArrayList<String>();
		relationSchemas = new ArrayList<Relation>();
		relationIDs = new HashMap<String,Integer>();
		IDToIndex = new HashMap<Integer,Integer>();
		//		relOffset = schema.relOffset;

		/*
			for (String k : schema.relationIDs.keySet())
				relationIDs.put(k, schema.relationIDs.get(k));

			for (String k : schema.relationNames)
				relationNames.add(k);

			for (Relation r: relationSchemas)
				relationSchemas.add(r.deepCopy());
		 */
		_schemaId = schema.getSchemaId();
		_description = schema.getDescription();
		Map<Relation,Relation> oldNewRel = new HashMap<Relation, Relation> ();
		for (Relation rel : schema.getRelations())
		{
			Relation relNew = rel.deepCopy();
			try
			{
				addRelation(relNew);
			} catch (DuplicateRelationIdException ex)
			{
				//TODO: Logger
				System.out.println ("Duplicate relation id should not occur in deep copy! : " + ex.getMessage());
				ex.printStackTrace();
			}
			oldNewRel.put(rel, relNew);
		}
		for (Map.Entry<Relation, Relation> entry : oldNewRel.entrySet())
			entry.getValue().deepCopyFks(entry.getKey(), this);

		finished = schema.finished;
	}


	public Relation getOrCreateRelationSchema(AbstractRelation rel) throws AbstractRelation.BadColumnName {
		if (getRelationSchema(rel.getName()) != null)
			return getRelationSchema(rel.getName());

		Relation rs = addRelation(rel.getName());
		for (RelationField field : rel.getFields()) {
			Type se = field.getType();
			rs.addCol(field.getName(), se);
		}
		return rs;
	}

	public Relation getRelationSchema(String relationName) {
		Integer index = relationIndices.get(relationName);
		if (index == null) {
			return null;
		}
		return relationSchemas.get(index);
	}

	/**
	 * Retrieves the schema with a given ID
	 * 
	 * @param id
	 * @return
	 */
	public Relation getRelationSchema(int id) {
		if (!IDToIndex.containsKey(id))
			return null;

		return relationSchemas.get(IDToIndex.get(id));// - relOffset);
	}

	/**
	 * Retrieves the relation indexed at index in the Schema
	 * 
	 * @param index
	 * @return
	 */
	public Relation getRelationSchemaAtIndex(int index) {
		return relationSchemas.get(index);
	}

	/*
	public void setOffset(int off) {
		relOffset = off;

		for (String nam : relationIDs.keySet()) {
			int inx = relationIDs.get(nam);
			relationSchemas.get(inx).setNewID(inx + relOffset);
		}
	}*/

	public synchronized Relation addRelation(String relationName) {
		if (finished) {
			throw new IllegalStateException("Cannot add relation to finished schema");
		}
		if (relationIndices.get(relationName) != null) {
			throw new IllegalArgumentException("Relation name " + relationName + " is already used by this schema");
		}
		int ID = SchemaIDBinding.getRelationId();//relationSchemas.size();
		relationIndices.put(relationName, relationSchemas.size());//ID);
		relationNames.add(relationName);
		relationIDs.put(relationName, ID);
		IDToIndex.put(ID, relationSchemas.size());

		Relation rs = new Relation(this, ID);// + relOffset);
		relationSchemas.add(rs);
		return rs;
	}

	/**
	 * Bind an existing relation to this schema
	 * 
	 * @param rel
	 * @return
	 */
	public synchronized int addRelation(Relation rel) throws DuplicateRelationIdException {
		if (finished) {
			throw new IllegalStateException("Cannot add relation to finished schema");
		}
		if (relationIndices.get(rel.getName()) != null) {
			throw new DuplicateRelationIdException(rel.getName(), getSchemaId());
		}
		int ID = SchemaIDBinding.getRelationId();//relationSchemas.size();
		relationIndices.put(rel.getName(), relationSchemas.size());//ID);
		relationNames.add(rel.getName());
		relationSchemas.add(rel);
		relationIDs.put(rel.getName(), ID);
		IDToIndex.put(ID, relationSchemas.size() - 1);

		rel.setSchema(this, ID);//ID + relOffset);

		Debug.println("Adding schema " + rel.getName() + " with ID " + ID+" with fields "+rel.getFields());

		return rel.getRelationID();//ID + relOffset;
	}

	public ArrayList<String> getRelationNames() {
		ArrayList<String> names = new ArrayList<String>();
		names.addAll(relationNames);
		return names;
	}

	public ArrayList<Relation> getRelationSchemas() {
		ArrayList<Relation> retval = new ArrayList<Relation>();
		retval.addAll(relationSchemas);
		return retval;
	}

	public int getNumRelations() {
		return relationNames.size();
	}

	public boolean isFinished() {
		return finished;
	}

	public void markFinished() {
		finished = true;
		for (Relation rs : relationSchemas) {
			if (!rs.isFinished()){
				rs.markFinished();
			}
		}
	}

	public String getNameForID(int relationID) {
		if (relationID != Relation.NO_ID) {
			Integer id = IDToIndex.get(Integer.valueOf(relationID));
			return relationNames.get(id);// - relOffset);
		} else {
			return "";
		}
	}

	public Integer getIDForName(String relationName) {
		//		if (!relationIDs.containsKey(relationName))
		//		return -1;

		//		return relationIDs.get(relationName) + relOffset;
		try {
			return getRelation(relationName).getRelationID();
		} catch (RelationNotFoundException rnf) {
			return -1;
		}
	}

	public Tuple getTupleFromBytes(byte[] bytes, int offset, int length) {
		final int bytesPerInt = IntType.bytesPerInt;
		int relId = IntType.getValFromBytes(bytes, offset);
		if (IDToIndex.get(relId) == null)
			return null;
		return new Tuple(relationSchemas.get(IDToIndex.get(relId)/* - relOffset*/), bytes, offset + bytesPerInt, length - bytesPerInt); 
	}

	public Tuple getTupleFromBytes(Relation r, byte[] bytes, int offset, int length) {
		final int bytesPerInt = IntType.bytesPerInt;
		IntType.getValFromBytes(bytes, offset);
		return new Tuple(r, bytes, offset + bytesPerInt, length - bytesPerInt); 
	}

	//@Override
	public boolean equalsREMOVE(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Schema s = (Schema) o;
		if (s.getNumRelations() != getNumRelations()) {
			return false;
		}

		for (int i = 0; i < getNumRelations(); ++i) {
			//Relation rs = getRelationSchema(i + relOffset);
			Relation rs = relationSchemas.get(i);
			Relation rs2 = s.getRelationSchema(rs.getRelationID());//i + s.relOffset);
			if (rs2 == null || ! rs.equals(rs2)) {
				return false;
			}
		}

		return true;
	}
	

	public String toStringText() {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < getNumRelations(); ++i) {
			Relation rs = getRelationSchema(i);
			sb.append(rs.getRelationName() + ": " + rs.toString() + "\n");
		}

		return sb.toString();
	}

	public Tuple createTuple(String relationName, Object... fields) throws ValueMismatchException {
		Relation rs = getRelationSchema(relationName);
		if (rs == null) {
			throw new IllegalArgumentException("Schema for relation " + relationName + " not found");
		}
		if (fields.length != rs.getNumCols()) {
			throw new IllegalArgumentException("Incorrect number of fields");
		}
		Tuple t = new Tuple(rs);

		for (int i = 0; i < fields.length; ++i) {
			if (fields[i] == null) {
				continue;
			} else if (fields[i] instanceof LabeledNull) {
				t.setLabeledNull(i, ((LabeledNull) fields[i]).getLabel());
			} else {
				t.set(i, fields[i]);
			}
		}
		t.setReadOnly();
		return t;
	}

	public Relation getSchema(String relationName) {
		Relation rs = getRelationSchema(relationName);
		if (rs == null) {
			throw new IllegalArgumentException("Schema for relation " + relationName + " not found");
		}
		return rs;
	}

	////////////////

	/**
	 * Get the relations contained in this schema
	 * @return Collection of relations
	 * @roseuid 449AEDB30109
	 */
	public synchronized Collection<Relation> getRelations() 
	{
		return relationSchemas;
	}

	/**
	 * Get a relation from it's name
	 * @param relName Relation name
	 * @return The relation, if found TODO: exception if doesn't exist
	 * @roseuid 449AEDC001A5
	 */
	public synchronized Relation getRelation(String relName) throws RelationNotFoundException
	{
		//TODO: Exception if relId unknown
		if (relationNames.contains(relName)){
			return relationSchemas.get(relationIndices.get(relName));
		}else{
			throw new RelationNotFoundException("Relation " + relName + " not found in Schema " + getSchemaId() + "(" + relationNames.toString() + ")");
		}
	}

	/**
	 * Get a relation found from it's underlying database characteristics :
	 * dbCatalog, dbSchema and dbRelationName
	 * @param dbCatal Database catalog containing the table
	 * @param dbSchema Database schema containing the table
	 * @param dbRelName Database table name
	 */
	public synchronized Relation getRelation (String dbCatal, String dbSchema, String dbRelName)
	{
		Relation res = null;
		Iterator<Relation> itRels = getRelations().iterator();
		while (itRels.hasNext() && res == null)
		{
			Relation rel = itRels.next();
			if (compareNullStr(dbCatal, rel.getDbCatalog())
					&& compareNullStr(dbSchema, rel.getDbSchema())
					&& dbRelName.equals(rel.getDbRelName()))
				res = rel;
		}
		return res;
		// TODO: exception if not found
	}

	private static boolean compareNullStr (String str1, String str2) {
		if (str1 == null) {
			return str2 == null;
		} else {
			return str1.equals(str2);
		}
	}


	//TODO: incoherence between DB which considers that relation name is unique for each schema and this 
	// class. Todo: distinguish a unique relation name from the fully qualified db name. 

	/**
	 * Test if a relation name exists in this schema
	 * @param relId Relation name
	 * @return true if it does exists
	 */
	public synchronized boolean existsRelation (String relName)
	{
		return relationNames.contains(relName);
	}

	/**
	 * Get the schema id
	 * @return Schema id
	 * @roseuid 44AD2C9700CB
	 */
	public String getSchemaId() 
	{
		return _schemaId;
	}

	/**
	 * Get the schema description
	 * @return Schema description
	 * @roseuid 44AD2CB10177
	 */
	public synchronized String getDescription() 
	{
		return _description;
	}

	/**
	 * Set the schema description
	 * @param description New description
	 */
	public synchronized void setDescription (String description)
	{
		_description  = description;
	}

	/**
	 * Returns a description of the schema, , conforms to the 
	 * flat file format defined in <code>RepositoryDAO</code>
	 * @return Schema description
	 */   
	public String toString ()
	{
		return toString (0);
	}

	/**
	 * Returns a description of the schema, conforms to the 
	 * flat file format defined in <code>RepositoryServer</code>.<BR>
	 * Description is indented according to the nb of tabulations
	 * @param nbTabs Number of tabulations in indentation
	 * @return Schema description
	 */   
	public synchronized String toString (int nbTabs)
	{

		StringBuffer buff = new StringBuffer (); 

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");

		buff.append ("SCHEMA ");
		buff.append (getSchemaId());
		String description = (getDescription() == null) ? "No description." : getDescription().replace("\n", "\\n");
		buff.append (" \"" + description + "\"\n");

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("\tRELATIONS\n");

		for (Relation rel : getRelations())
			buff.append(rel.toString(nbTabs+2) + "\n");

		return buff.toString();	   
	}

	/**
	 * Get a deep copy of this schema.
	 * @return Deep copy
	 * @see Schema#Schema(Schema)
	 */
	public synchronized Schema deepCopy ()
	{
		return new Schema (this);
	}

	public synchronized void serialize(Document doc, Element schema) {
		schema.setAttribute("name", _schemaId);	   
		DomUtils.addChildWithText(doc,schema, "description", getDescription());
		for (String key : relationNames) {
			Element rel = DomUtils.addChild(doc, schema, "relation");
			Relation r = relationSchemas.get(relationIndices.get(key));
			r.serialize(doc, rel);
			for (ForeignKey fk : r.getForeignKeys()) {
				Element foreignkey = DomUtils.addChild(doc, schema, "foreignkey");
				fk.serialize(doc, foreignkey);
			}
		}
	}

	public void createMissingRelations() {
		boolean needLocals = true;
		boolean needRejects = true;
		for (Relation rel : getRelations()) {
			if (rel.getDbRelName().endsWith(Relation.LOCAL))
				needLocals = false;
			if (rel.getDbRelName().endsWith(Relation.REJECT))
				needRejects = false;
		}

		int n = getRelations().size();
		if (needLocals) {
			for (int i = 0; i < n; i++) {
				Relation rel = getRelationSchemaAtIndex(i);
				if(rel.hasLocalData()){
					Relation rel2 = rel.deepCopy(rel.getLocalName(),
							rel.getLocalInsDbName(), rel.getDbSchema());
					try {
						Debug.println("ADD RELATION: " + rel2);
						addRelation(rel2);

					} catch (DuplicateRelationIdException de) {

					}
				}
			}
		}
		if (needRejects && Config.getRejectionTables()) {
			for (int i = 0; i < n; i++) {
				Relation rel = getRelationSchemaAtIndex(i);
				Relation rel2 = rel.deepCopy(rel.getRejectName(),
						rel.getLocalRejDbName(), rel.getDbSchema());
				try {
					Debug.println("ADD RELATION: " + rel2);
					addRelation(rel2);
				} catch (DuplicateRelationIdException de) {

				}
			}
		}
	}

	public static Schema deserialize(Element schema, boolean builtinFunction) throws DuplicateRelationIdException, UnknownRefFieldException, XMLParseException, UnsupportedTypeException, RelationNotFoundException {
		String id = schema.getAttribute("name");

		String desc = "";
		Element descElt = DomUtils.getChildElementByName(schema, "description");	   
		if (descElt != null)
			desc = descElt.getTextContent();
		Schema s = new Schema(id, desc);

		for (Element rel : DomUtils.getChildElementsByName(schema, "relation")) {
			Relation r = Relation.deserialize(rel, builtinFunction);
			s.addRelation(r);
		}
		for (Element key : DomUtils.getChildElementsByName(schema, "foreignkey")) {
			ForeignKey fk = ForeignKey.deserialize(s, key);
			String rel = fk.getRelation();
			s.getRelation(rel).finished = false;
			s.getRelation(rel).addForeignKey(fk);
			s.getRelation(rel).finished = true;
		}
		s.createMissingRelations();
		s.markFinished();
		return s;
	}

	/**
	 * Remaps the relation IDs, if they have been changed
	 */
	public void resetIDs() {
		for (int i = 0; i < relationSchemas.size(); i++) {
			Relation r = relationSchemas.get(i);
			IDToIndex.put(r.getRelationID(), i);
		}
	}
	public void replaceRelation(Relation rel){
		String relationName = rel.getRelationName();
		if (finished) {
			throw new IllegalStateException("Cannot add relation to finished schema");
		}
		if (relationIndices.get(relationName) == null) {
			throw new IllegalArgumentException("Relation name " + relationName + " is not being used by this schema");
		}
		int pos = relationIndices.get(relationName);

		relationSchemas.remove(pos);
		relationSchemas.add(pos, rel);

	}

	/**
	 * DOCUMENT ME
	 * 
	 * @param schema
	 * @param builtinFunction
	 * @param cdss
	 * @param peerID
	 * @param schemaIDBindingClient
	 * @return
	 * @throws UnsupportedTypeException 
	 * @throws XMLParseException 
	 * @throws UnknownRefFieldException 
	 * @throws DuplicateRelationIdException 
	 * @throws RelationNotFoundException 
	 * @throws USException 
	 */
	public static Schema deserialize(Element schema, boolean builtinFunction,
			String cdss, AbstractPeerID peerID,
			ISchemaIDBindingClient schemaIDBindingClient)
			throws UnknownRefFieldException, XMLParseException,
			UnsupportedTypeException, DuplicateRelationIdException,
			RelationNotFoundException, USException {
		String id = schema.getAttribute("name");

		String desc = "";
		Element descElt = DomUtils.getChildElementByName(schema, "description");
		if (descElt != null)
			desc = descElt.getTextContent();
		Schema s;
		boolean save = false;
		s = schemaIDBindingClient.getSchema(cdss, peerID, id);

		if (s == null) {
			s = new Schema(id, desc);
			logger.debug("Created new schema " + id + " for " + peerID + ".");
			save = true;
		}
		ArrayList<String> existingRelations = s.getRelationNames();
		for (Element rel : DomUtils.getChildElementsByName(schema, "relation")) {
			if (!existingRelations.contains(rel.getAttribute("name"))) {
				Relation r = Relation.deserialize(rel, builtinFunction);
				s.addRelation(r);
				logger.debug("Created new relation " + r.getName()
						+ " for peer " + peerID + ", schema " + id + ".");
				save = true;
			}

		}
		for (Element key : DomUtils
				.getChildElementsByName(schema, "foreignkey")) {
			ForeignKey fk = ForeignKey.deserialize(s, key);
			String rel = fk.getRelation();
			s.getRelation(rel).finished = false;
			s.getRelation(rel).addForeignKey(fk);
			s.getRelation(rel).finished = true;
		}
		s.createMissingRelations();
		// TODO persist to schemaIDBinding if any changes?
		return s;
	}
	
}
