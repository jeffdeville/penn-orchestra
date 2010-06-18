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

import static edu.upenn.cis.orchestra.util.DomUtils.getBooleanAttribute;

import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Peer definition
 * @author Olivier Biton
 *****************************************************************
 */
public class Peer 
{
	/** Peer id, should be unique for the whole system */
	private String _id;
	/** Peer id object */
	private StringPeerID _peerId;
	/** Peer address, depending on the implementation. Could be IP:Port... */
	private String _address;
	/** Peer description, for final users */
	private String _description;
	/** Schemas defined for this peer */
	private Map<String, Schema> _schemas= new Hashtable<String, Schema>();
	/** Mappings defined for this peer */
	private Map<String, Mapping> _mappings = new Hashtable<String, Mapping> ();
	/** Indicates if this Peer is the local peer for a system. */
	private final boolean _isLocalPeer;


	/**
	 * Create a new peer
	 * @param id Peer id, should be unique for the whole system
	 * @param address Peer address, depending on the implementation. Could be IP:Port...
	 * @param description Peer description, for final users
	 * @see OrchestraSystem#addPeer(Peer)
	 */
	public Peer (String id, String address, String description)
	{
		this(id, address, description, false);
	}
	
	/**
	 * Create a new peer
	 * @param id Peer id, should be unique for the whole system
	 * @param address Peer address, depending on the implementation. Could be IP:Port...
	 * @param description Peer description, for final users
	 * @param isLocalPeer 
	 * @see OrchestraSystem#addPeer(Peer)
	 */
	public Peer (String id, String address, String description, boolean isLocalPeer)
	{
		_id = id;
		_peerId = new StringPeerID(_id);
		_address = address;
		_description = description;
		_isLocalPeer = isLocalPeer;
	}


	/**
	 * Deep copy of the peer <BR>
	 * Use the method deepCopy to benefit from polymorphism <BR>
	 * <U>WARNING:</U> Will not copy the mappings
	 * @param peer Peer to copy
	 * @roseuid 449AEA650271
	 * @see Peer#deepCopy()
	 * @see Peer#deepCopyMappings(Peer, OrchestraSystem)
	 * @see OrchestraSystem#deepCopy()
	 */  
	protected Peer (Peer peer)
	{
		_id = peer.getId();
		_peerId = peer.getPeerId();
		_address = peer.getAddress();
		_description = peer.getDescription();
		_isLocalPeer = peer._isLocalPeer;
		
		for (Schema sch : peer.getSchemas())
		{
			try
			{
				addSchema(sch.deepCopy());
			} catch (DuplicateSchemaIdException ex)
			{
				//TODO: Logger
				// Problem could come if someone calls getSchemas which is not a deep copy and adds something
				// to this list
				System.out.println ("Duplicate schema id error should not occur for a deep copy! " + ex.getMessage());
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Add a new schema to this peer (schemaId must be unique for this peer) 
	 * @param schema The new schema
	 * @throws DuplicateSchemaIdException If the schema id is already used for another schema
	 */
	public synchronized void addSchema (Schema schema)
	throws DuplicateSchemaIdException
	{
		if (_schemas.containsKey(schema.getSchemaId()))
			throw new DuplicateSchemaIdException (getId(), schema.getSchemaId());
		_schemas.put (schema.getSchemaId(), schema);
	}


	/**
	 * Get the peer unique Id 
	 * @return java.lang.String Peer id, should be unique for the whole system
	 * @roseuid 4496D67B02EE
	 */
	public String getId()
	{
		return _id;
	}

	/**
	 * Get the peer address 
	 * @return Peer address, depending on the P2P implementation. Could be IP:Port...
	 */
	public String getAddress ()
	{
		return _address;
	}

	/** 
	 * Get the peer description
	 * @return Peer description, for final users
	 */
	public String getDescription ()
	{
		return _description;
	}

	/**
	 * Get the list of schemas defined for this peer 
	 * @return List Schemas defined for this peer
	 * @roseuid 4496D69D02CE
	 */
	public synchronized Collection<Schema> getSchemas()
	{
		return _schemas.values();
	}

	/**
	 * Get one of the peer schemas from its Id
	 * @param String id Schema id 
	 * @return The schema if it exists
	 * @roseuid 449AECFC02FD
	 */
	public synchronized Schema getSchema(String id)
	{
		//TODO: Exception if it doesn't exist		
		return _schemas.get(id);
	}

	/**
	 * Get the idss of all of this peer's schemas
	 * 
	 * @return	The set of schema ids
	 */
	public synchronized Set<String> getSchemaIds() {
		return new HashSet<String>(_schemas.keySet());
	}

	/**
	 * Remove a given schema.<BR>
	 * No error if this schema id was unknown
	 * @param schemaId Id of the schema to remove
	 */
	public synchronized void removeSchema (String schemaId)
	{
		_schemas.remove(schemaId);
	}

	// TODO: Exception if mapping not defined on peer??

	/**
	 * Add a new mapping to this peer.
	 * @param mapping Mapping to be added
	 * @throws DuplicateMappingIdException If the mapping if is already used
	 */
	public synchronized void addMapping (Mapping mapping)
	throws DuplicateMappingIdException
	{
		if (getMapping(mapping.getId())!=null)
			throw new DuplicateMappingIdException (getId(), mapping.getId());
		else
			_mappings.put(mapping.getId(), mapping);
	}

	/**
	 * Get the list of mappings related to this peer
	 * WARNING: this is not a deep copy
	 * @return List of mappings
	 */
	public synchronized Collection<Mapping> getMappings ()
	{
		return _mappings.values();
	}


	/**
	 * Get a mapping from its id
	 * @return The mapping if found, null otherwise
	 */
	public synchronized Mapping getMapping (String mappingId)
	{
		return _mappings.get(mappingId);
	}

	/**
	 * Remove a mapping
	 * No exception raised if mapping doesn't exist
	 * @param mappingId Id of mapping to remove
	 */
	public synchronized void removeMapping (String mappingId)
	{
		_mappings.remove(mappingId);
	}

	/**
	 * Get a deep copy of this peer. <BR>
	 * WARNING: Will not copy the mappings, to copy the mappings 
	 * in a deep copy, ask for a deep copy of the OrchestraSystem
	 * @return Deep copy of this peer
	 * @see OrchestraSystem#deepCopy()
	 * @see Peer#deepCopyMappings(Peer, OrchestraSystem)
	 */   
	public synchronized Peer deepCopy ()
	{
		return new Peer (this);
	}


	//TODO: Exception if it's not the same peer
	/**
	 * To complete a peer deep copy, deep copy of the mappings 
	 * @param p Peer from which the deep copy is to be made
	 * @param system System containing all the peers after deep copy (including this peer)
	 * @see Peer#deepCopy()
	 */
	protected synchronized void deepCopyMappings (Peer p, OrchestraSystem system)
	{
		try
		{
			for (Mapping mapp : p.getMappings())
				addMapping(mapp.deepCopy(system));
		} catch (DuplicateMappingIdException ex)
		{
			// Should not happen during a deep copy, no need to expose client to this error
			//TODO Logger+terminate
			System.out.println ("UNEXPECTED ERROR: " + ex.getMessage());
			ex.printStackTrace();
		}

	}

	public StringPeerID getPeerId() {
		return _peerId;
	}
	
	/**
	 * String representation of this Peer.
	 * Conforms with the flat file representation defined in <code>RepositoryServer</code>
	 * @return String representation
	 */
	public String toString ()
	{
		return toString(0);
	}

	/**
	 * String representation of this Peer, indented with <code>nbTabs</code> tabs.
	 * Conforms with the flat file representation defined in <code>RepositoryDAO</code>
	 * @param nbTabs Number of tabs for indentation
	 * @return String representation
	 */

	protected synchronized String toString (int nbTabs)
	{
		StringBuffer buff = new StringBuffer (); 

		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("PEER ");
		buff.append(getId());
		buff.append(" ");
		buff.append(getAddress());
		buff.append(" ");
		buff.append("\"" + getDescription().replace("\n", "\\n") + "\"");
		buff.append("\n");
		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		buff.append("\tSCHEMAS\n");
		for (Schema schema : getSchemas())
			buff.append (schema.toString(nbTabs+2));
		buff.append ("\n");
		for (int i = 0 ; i < nbTabs ; i++)
			buff.append("\t");
		if (getMappings().size()>0)
			buff.append("\tMAPPINGS\n");
		for (Mapping mapp : getMappings())
		{
			for (int i = 0 ; i < nbTabs +2 ; i++)
				buff.append("\t");
			buff.append (mapp.toString() + "\n");
		}


		return buff.toString();	   
	}


	public static Peer deserialize(Element peer) throws DuplicateSchemaIdException, 
	DuplicateRelationIdException, UnknownRefFieldException, 
	DuplicateMappingIdException, XMLParseException, UnsupportedTypeException, RelationNotFoundException {
		String id = peer.getAttribute("name");
		String addr = peer.getAttribute("address");
		Element descrElt = DomUtils.getChildElementByName(peer, "description");
		String descr="";
		if (descrElt != null)
			descr = descrElt.getTextContent();
		Peer p = new Peer(id, addr, descr);
		for (Element schema : DomUtils.getChildElementsByName(peer, "schema")) {
			Schema s = Schema.deserialize(schema, false);
			p.addSchema(s);
		}
		return p;
	}

	public synchronized void serialize(Document doc, Element peer) {
		peer.setAttribute("name", _id);
		peer.setAttribute("address", _address);
		DomUtils.addChildWithText(doc, peer, "description", getDescription());
		for (String key : _schemas.keySet()) {
			Element schema = DomUtils.addChild(doc, peer, "schema");
			Schema s = _schemas.get(key);
			s.serialize(doc, schema);
		}
		//dinesh ++
		//Commented to avoid adding mapping tag to individual peers
		/*for (String key : _mappings.keySet()) {
			Element mapping = DomUtils.addChild(doc, peer, "mapping");
			_mappings.get(key).serialize(doc, mapping);
		}*/
		//dinesh --
	}


	/**
	 * DOCUMENT ME
	 * 
	 * @param pe
	 * @param cdss
	 * @param schemaIDBindingClient
	 * @return
	 * @throws RelationNotFoundException
	 * @throws UnsupportedTypeException
	 * @throws XMLParseException
	 * @throws UnknownRefFieldException
	 * @throws DuplicateRelationIdException
	 * @throws DuplicateSchemaIdException
	 * @throws USException 
	 */
	public static Peer deserialize(Element pe, String cdss,
			ISchemaIDBindingClient schemaIDBindingClient)
			throws DuplicateRelationIdException, UnknownRefFieldException,
			XMLParseException, UnsupportedTypeException,
			RelationNotFoundException, DuplicateSchemaIdException, USException {
		String id = pe.getAttribute("name");
		String addr = pe.getAttribute("address");
		Element descrElt = DomUtils.getChildElementByName(pe, "description");
		String descr = "";
		if (descrElt != null)
			descr = descrElt.getTextContent();
		Peer p = new Peer(id, addr, descr);
		for (Element schema : DomUtils.getChildElementsByName(pe, "schema")) {
			Schema s = Schema.deserialize(schema, false, cdss, p.getPeerId(),
					schemaIDBindingClient);
			p.addSchema(s);
		}
		return p;
	}


	/**
	 * Returns the {@code Peer} represented by {@code peerElement}, ignoring all
	 * {@code schema} children of {@code peerElement};
	 * 
	 * @param peerElement
	 * @return a {@code Peer} with no {@code Schema} attached.
	 */
	public static Peer deserializePeerNoChildren(Element peerElement) {
		String id = peerElement.getAttribute("name");
		String addr = peerElement.getAttribute("address");
		boolean isLocal = getBooleanAttribute(peerElement, "localPeer");
		Element descrElt = DomUtils.getChildElementByName(peerElement,
				"description");
		String descr = "";
		if (descrElt != null)
			descr = descrElt.getTextContent();
		Peer p = new Peer(id, addr, descr, isLocal);
		return p;
	}
	
	/**
	 * Returns {@code true} if this Peer is the localPeer for it's {@code OrchestraSystem}. 
	 * 
	 * @return {@code true} if this Peer is the localPeer for it's {@code OrchestraSystem}
	 */
	public boolean isLocalPeer() {
		return _isLocalPeer;
	}
}
