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

import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;

import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;



/**
 * The peer/schema/relation... object hierarchy has been designed to avoid circular dependencies.
 * Thus a relation does not have a reference to its schema, nor has a schema to its peer...
 * But in some cases we need to have this information (we want to avoid a lookup for the relation
 * in the OrchestraSystem). That's what this class will be used for, typically for Mappings atoms.
 * Note: this class is just a container, it will not check that the schema contains the relation or that
 * the peer contains the schema
 * 
 * @author Olivier Biton
 *
 */
public class RelationContext 
{
	/** The relation itself */
	private Relation _relation;
	
	/** The relation's schema */	
	private Schema _schema;
	
	/** The schema's peer */
	private Peer _peer;
	
	private boolean _mapping;
	
	public RelationContext (Relation relation, Schema schema, Peer peer, boolean mapping)
	{
		this._peer = peer;
		this._schema = schema;
		this._relation = relation;		
		this._mapping = mapping;
	}

	public RelationContext (Peer peer, Schema schema, Relation relation)
	{
		this._peer = peer;
		this._schema = schema;
		this._relation = relation;		
		this._mapping = false;
	}

	public boolean isMapping () {
		return _mapping;
	}
		
	public Peer getPeer() {
		return _peer;
	}

	public Relation getRelation() {
		return _relation;
	}

	public Schema getSchema() {
		return _schema;
	}
	
	
	public String toString ()
	{
		if(_peer != null && _schema != null)
			return _peer.getId() + "." + _schema.getSchemaId() + "." + _relation.getName();
		else
			return _relation.getName();
	}
	
	@Override
	public boolean equals (Object relContext)
	{
		if (relContext instanceof RelationContext)
		{
			return equals((RelationContext) relContext);
		}
		else
			return false;
	}		
	
	/*
	public boolean equals (RelationContext relContext)
	{
		return (getRelation() == relContext.getRelation()
				&& getSchema() == relContext.getSchema()
				&& getPeer() == relContext.getPeer());	
	}*/
	
	public boolean equals (RelationContext relContext)
	{
		return ((getRelation() == relContext.getRelation() || getRelation().getName().equals(relContext.getRelation().getName()))
				&& (getSchema() == relContext.getSchema() || getSchema().getSchemaId().equals(relContext.getSchema().getSchemaId()))
				&& (getPeer() == relContext.getPeer() || getPeer().getId().equals(relContext.getPeer().getId())));	
	}
	
	@Override
	public int hashCode() {
		int code = 17;
		code = 31 * code + getRelation().getName().hashCode();
		code = 31 * code + getSchema().getSchemaId().hashCode();
		code = 31 * code + getPeer().getId().hashCode();
		return code;
		//return getRelation().hashCode() ^ getSchema().hashCode();
	}
	
	/**
	 * Returns an XML {@code Element} representing this {@code RelationContext}.
	 * 
	 * @param doc only used to create the {@code Element}.
	 * @return an XML {@code Element} representing this {@code RelationContext} 
	 */
	public Element serialize(Document doc) {
		Element e = doc.createElement("relationContext");
		e.setAttribute("mapping", Boolean.toString(_mapping));
		e.setAttribute("peer", _peer.getId());
		e.setAttribute("schema", _schema.getSchemaId());
		e.setAttribute("relation", _relation.getName());

		return e;
	}

	/**
	 * Returns the {@code RelationContext} represented by {@code
	 * relationContext}.
	 * 
	 * @param relationContext
	 * @param system
	 * @return the {@code RelationContext} represented by {@code
	 *         relationContext}
	 * @throws XMLParseException
	 */
	public static RelationContext deserialize(Element relationContext,
			OrchestraSystem system) throws XMLParseException {

		String peerName = relationContext.getAttribute("peer");

		Peer peer = system.getPeer(peerName);
		if (peer == null) {
			throw new XMLParseException("Peer [" + peerName
					+ "] not found in system " + system.getName());
		}

		String schemaName = relationContext.getAttribute("schema");
		Schema schema = peer.getSchema(schemaName);
		if (schema == null) {
			throw new XMLParseException("Schema [" + schemaName
					+ "] not found in peer " + peerName);
		}

		String relationName = relationContext.getAttribute("relation");

		RelationContext result = null;
		try {
			result = system.getRelationByName(peerName, schemaName,
					relationName);
		} catch (RelationNotFoundException e) {
			throw new XMLParseException("Relation [" + relationName
					+ "] not found in schema " + schemaName, e);
		}
		return result;
	}
	
	/**
	 * Returns an XML representation of {@code relations}.
	 * 
	 * @param userRelationsDoc only used for element creation.
	 * @param relations
	 * @param string
	 * @return an XML representation of {@code relations}
	 */
	public static Element serialize(Document userRelationsDoc,
			List<RelationContext> relations, String string) {
		Element e = userRelationsDoc.createElement(string);
		for (RelationContext relation : relations) {
			e.appendChild(relation.serialize(userRelationsDoc));
		}
		return e;
	}
}
