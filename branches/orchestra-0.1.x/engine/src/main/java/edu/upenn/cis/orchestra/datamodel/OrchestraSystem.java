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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.DatalogViewUnfolder;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.RecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.NoLocalPeerException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdater;
import edu.upenn.cis.orchestra.localupdates.LocalUpdaterFactory;
import edu.upenn.cis.orchestra.localupdates.exceptions.NoLocalUpdaterClassException;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.NoExtractorClassException;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.exceptions.RecursionException;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.NotPred;
import edu.upenn.cis.orchestra.predicate.OrPred;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/****************************************************************
 * Class used to group multiple peers (global view of the system)
 * 
 * @author Olivier Biton
 ***************************************************************** 
 */
public class OrchestraSystem {

	// Subclasses that must be loaded before we can deserialize anything
	@SuppressWarnings("unused")
	private static Class<?>[] classesToLoad = { IntPeerID.class,
			StringPeerID.class, AndPred.class, OrPred.class, NotPred.class,
			ComparePredicate.class };

	/** Set of peers composing the system */
	private Map<String, Peer> _peers;
	/**
	 * The local peer for this system. All publishing and reconciling will be
	 * done with respect to this peer.
	 */
	private Peer _localPeer;
	/** This will handle any updates to the local peer. */
	private ILocalUpdater _localUpdater;
	private static Logger _logger = LoggerFactory
			.getLogger(OrchestraSystem.class);

	/** Local objects: mapping and reconciliation engines */
	protected BasicEngine _mappingEngine;
	private UpdateStore.Factory _usf;
	private StateStore.Factory _ssf;
	private Map<String, Db> _recDbs;
	private ISchemaIDBinding _mapStore;
	private Map<String, Map<String, TrustConditions>> _tcs;
	// protected
	// Map<String,Map<String,edu.upenn.cis.orchestra.datamodel.Schema>> _schemas
	// = new
	// Hashtable<String,Map<String,edu.upenn.cis.orchestra.datamodel.Schema>>();

	// protected Schema _schema;
	private Map<Peer, Schema> _schemas;
	private boolean _recMode;
	private String _name = "";
	/**
	 * Indicates that this System contains bidirectional mappings.
	 */
	private boolean _bidirectional;

	/**
	 * Creates an empty Orchestra system
	 */
	public OrchestraSystem() {
		super();
		_peers = new Hashtable<String, Peer>();
		_recDbs = new Hashtable<String, Db>();
		_schemas = new HashMap<Peer, Schema>();
		_tcs = new Hashtable<String, Map<String, TrustConditions>>();
	}

	public OrchestraSystem(ISchemaIDBinding sch) {
		this();
		_mapStore = sch;
	}

	/**
	 * Deep copy of a given OrchestraSystem (use method deepCopy to benefit from
	 * polymorphism)
	 * 
	 * @param system System to copy
	 * @see OrchestraSystem#deepCopy()
	 */
	protected OrchestraSystem(OrchestraSystem system) {
		this(system._mapStore);

		// First: Create deep copy of peers without their
		// mappings (would'nt be possible to load mappings
		// if the referenced peer does not exist yet)
		Map<Peer, Peer> oldNewPeers = new HashMap<Peer, Peer>();
		for (Peer p : system.getPeers()) {
			Peer np = p.deepCopy();
			if (system.isLocalPeer(p)) {
				_localPeer = np;
			}
			oldNewPeers.put(p, np);
			try {
				addPeer(np);
			} catch (DuplicatePeerIdException ex) {
				System.out
						.println("Peer id conflict should not happen in a deep copy!");
				ex.printStackTrace();
			}
		}
		_bidirectional = system._bidirectional;
		// Second: Complete the deep copy by copying the peers
		for (Map.Entry<Peer, Peer> entry : oldNewPeers.entrySet())
			entry.getValue().deepCopyMappings(entry.getKey(), this);

		// Third: shallow-copy the stuff more recently added to OrchestraSystem
		// TODO: deep-copy?
		_mappingEngine = system._mappingEngine;
		// _mappingDb = system._mappingDb;
		_usf = system._usf;
		_ssf = system._ssf;
		for (String peer : system._tcs.keySet()) {
			Map<String, TrustConditions> tcForSchemas = new HashMap<String, TrustConditions>();
			_tcs.put(peer, tcForSchemas);
			for (String schema : system._tcs.get(peer).keySet()) {
				tcForSchemas.put(schema, system._tcs.get(peer).get(schema));
			}
		}
		_recMode = system._recMode;
		_name = system._name;
		// _recDbs will get filled in as needed
	}

	public synchronized String getName() {
		return _name;
	}

	public synchronized void setName(String name) {
		_name = name;
	}

	public synchronized boolean getRecMode() {
		return _recMode;
	}

	public synchronized void setRecMode(boolean recMode) {
		_recMode = recMode;
	}

	// TODO: Errors if ids not unique

	/**
	 * Add a new peer to the system
	 * 
	 * @param peer peer to add
	 * @throws DuplicatePeerIdException If a peer already exists with the same
	 *             id
	 */
	public synchronized void addPeer(Peer peer) throws DuplicatePeerIdException {
		if (_peers.containsKey(peer.getId()))
			throw new DuplicatePeerIdException(peer.getId());
		else
			_peers.put(peer.getId(), peer);

		/*
		 * Map<String,edu.upenn.cis.orchestra.datamodel.Schema> map = new
		 * Hashtable<String,edu.upenn.cis.orchestra.datamodel.Schema>();
		 * _schemas.put(peer.getId(), map); for (Schema s : peer.getSchemas()) {
		 * try { map.put(s.getSchemaId(), new
		 * edu.upenn.cis.orchestra.datamodel.Schema(s)); } catch (BadColumnName
		 * bcn) { bcn.printStackTrace(); } }
		 */
	}

	/**
	 * Add a list of peers to the system
	 * 
	 * @param peers new peers
	 * @throws DuplicatePeerIdException If at least one peer in
	 *             <code>peers</code> uses an id already used in the system, or
	 *             if at least two peers in this list have a common id.
	 * @throws NoLocalPeerException
	 */
	public void addPeers(List<Peer> peers) throws DuplicatePeerIdException,
			NoLocalPeerException {
		for (Peer p : peers) {
			addPeer(p);
			if (p.isLocalPeer()) {
				if (_localPeer == null) {
					_localPeer = p;
				} else {
					throw new NoLocalPeerException(_localPeer, p);
				}
			}
		}
		if (_localPeer == null) {
			throw new NoLocalPeerException();
		}
	}

	/**
	 * Get a peer from its id
	 * 
	 * @param peerId Id to look for
	 * @return Peer if it exists, null otherwise
	 */
	public synchronized Peer getPeer(String peerId) {
		return _peers.get(peerId);
	}

	/**
	 * Get the list of peers in this system <br>
	 * WARNING: Not a deep copy (to improve performances)
	 * 
	 * @return List of peers, can be empty, cannot be null
	 */
	public synchronized Collection<Peer> getPeers() {
		List<Peer> retval = new ArrayList<Peer>(_peers.values());
		Collections.sort(retval, new Comparator<Peer>() {
			public int compare(Peer o1, Peer o2) {
				return o1.getId().compareTo(o2.getId());
			}
		});
		return retval;
	}

	// TODO: Check if mappings ref this peer!
	/**
	 * Removes the peer from the system
	 * 
	 * @param peerId Id of the peer to remove
	 */
	public synchronized void removePeer(String peerId) {
		_peers.remove(peerId);
	}

	/**
	 * Get a deep copy of this Orchestra system
	 * 
	 * @return Deep copy
	 * @throws
	 * @see OrchestraSystem#OrchestraSystem(OrchestraSystem)
	 */
	public synchronized OrchestraSystem deepCopy() {
		return new OrchestraSystem(this);
	}

	/**
	 * String representation of this Orchestra system. <BR>
	 * Conforms with the flat file representation defined in RepositoryDAO
	 * 
	 * @return String representation
	 */
	public synchronized String toString() {

		StringBuffer buff = new StringBuffer();
		buff.append("PEERS\n");
		for (Peer p : getPeers())
			buff.append(p.toString(1) + "\n");
		return buff.toString();
	}

	/**
	 * Extract mappings from all the system peers
	 * 
	 * @param materialized If true, only materialized mappings will be returned.
	 *            If false only not materialized
	 * @return Mappings
	 */
	public synchronized List<Mapping> getAllSystemMappings(boolean materialized) {
		List<Mapping> res = new ArrayList<Mapping>();
		for (Peer p : getPeers())
			for (Mapping mapping : p.getMappings())
				if (mapping.isMaterialized() == materialized)
					res.add(mapping);
		return res;
	}

	public synchronized List<RelationContext> getAllUserRelations() {
		List<RelationContext> rels = new ArrayList<RelationContext>();

		for (Peer p : getPeers()) {
			for (Schema s : p.getSchemas()) {
				for (Relation r : s.getRelations()) {
					if (!r.isInternalRelation()) {
						rels.add(new RelationContext(r, s, p, false));
					}
				}
			}
		}
		return rels;
	}

	public synchronized RelationContext getRelationByName(String peer,
			String schema, String relation) throws RelationNotFoundException {
		Peer p = _peers.get(peer);
		if (p != null) {
			Schema s = p.getSchema(schema);
			if (s != null) {
				Relation r = null;
				try {
					r = s.getRelation(relation);
				} catch (RelationNotFoundException e) {
					// We don't want to throw an exception here without first
					// checking the mapping relations below.
				}
				if (r != null) {
					return new RelationContext(r, s, p, false);
				}
			}
		}
		// also look in the mapping relations
		if (_mappingEngine != null
				&& _mappingEngine.getMappingRelations() != null) {
			for (RelationContext scr : _mappingEngine.getMappingRelations()) {
				if (scr.getPeer().getId().equals(peer)
						&& scr.getSchema().getSchemaId().equals(schema)
						&& scr.getRelation().getName().equals(relation)) {
					return scr;
				}
			}
		}
		if (getMappingDb().getBuiltInSchemas().containsKey(schema)) {
			Relation rel = getMappingDb().getBuiltInSchemas().get(schema)
					.getRelation(relation);
			if (rel != null)
				return new RelationContext(rel, getMappingDb()
						.getBuiltInSchemas().get(schema), null, false);
		}
		throw new RelationNotFoundException(peer + "." + schema + "."
				+ relation);
	}

	public synchronized List<RelationContext> getRelationsByName(String relation) {
		ArrayList<RelationContext> list = new ArrayList<RelationContext>();
		for (Peer p : getPeers()) {
			for (Schema s : p.getSchemas()) {
				for (Relation r : s.getRelations()) {
					if (r.getName().equals(relation)) {
						list.add(new RelationContext(r, s, p, false));
					}
				}
			}
		}
		for (Schema sch : getMappingDb().getBuiltInSchemas().values()) {
			try {
				Relation rel = sch.getRelation(relation);
				list.add(new RelationContext(rel, sch, null, false));
			} catch (RelationNotFoundException rnf) {

			}
		}
		if (_mappingEngine != null) {
			for (RelationContext scr : _mappingEngine.getMappingRelations()) {
				if (scr.getRelation().getName().equals(relation)) {
					list.add(scr);
				}
			}
		}
		return list;
	}

	public synchronized Schema getSchemaByName(String peer, String schema) {
		Peer p = _peers.get(peer);
		if (p != null) {
			return p.getSchema(schema);
		} else
			return getMappingDb().getBuiltInSchemas().get(schema);

		// return null;
	}

	public synchronized List<Schema> getAllSchemas() {
		List<Schema> ret = new ArrayList<Schema>();
		for (Peer p : getPeers())
			ret.addAll(p.getSchemas());
		return ret;
	}

	public synchronized Db getRecDb(String name) throws DbException {
		// if (! getRecMode()) {
		// throw new DbException("Cannot get a RecDb when not in Rec mode");
		// }
		Peer p = _peers.get(name);
		if (p == null) {
			throw new IllegalArgumentException("Peer " + name
					+ " is not in the Orchestra system");
		}
		if (!p.isLocalPeer()) {
			throw new DbException("May not access non-local peer's " + name + " database.");
		}
		/*
		 * if (_schemas.get(name) == null || _schemas.get(name).size() != 1) {
		 * throw new
		 * IllegalArgumentException("Cannot get a reconciliation DB for peer " +
		 * name + " without exactly one schema"); }
		 * edu.upenn.cis.orchestra.datamodel.Schema schema = null; for
		 * (edu.upenn.cis.orchestra.datamodel.Schema s :
		 * _schemas.get(name).values()) { schema = s; } String schemaName =
		 * null; for (String sn : _schemas.get(name).keySet()) { schemaName =
		 * sn; } if (_schema == null) { _schema = schema; } else { if (!
		 * _schema.equals(schema)) { throw new
		 * DbException("Cannot create reconciliation DBs with different schemas"
		 * ); } }
		 */

		// if (_schema == null) {
		/*
		 * Schema schema = new
		 * Schema(p.getSchemas().iterator().next().getSchemaId(),
		 * p.getSchemas().iterator().next().getDescription());
		 * 
		 * // Give every schema the same name for (Schema sch : getAllSchemas())
		 * { for (Relation r : sch.getRelations()) try { schema.addRelation(r);
		 * } catch (DuplicateRelationIdException dri) { } }
		 * schema.markFinished();
		 */
		// }
		// schema = p.getSchemas().iterator().next();
		Schema schema = p.getSchemas().iterator().next();
		// _schemas.put(p, schema);
		String schemaName = schema.getSchemaId();
		Db db = _recDbs.get(name);
		if (db == null) {
			if (_tcs.get(name) != null
					&& _tcs.get(name).get(schemaName) != null) {
				db = new ClientCentricDb(_mapStore, schema, new StringPeerID(
						name), _tcs.get(name).get(schemaName), _usf, _ssf);
			} else {
				db = new ClientCentricDb(_mapStore, schema, new StringPeerID(
						name), _usf, _ssf);
			}
			_recDbs.put(name, db);
			return db;
		} else {
			if (!db.isConnected()) {
				db.reconnect();
			}
			return db;
		}
	}

	public boolean isLocalUpdateStore() {
		return _usf.isLocal();
	}

	public synchronized BasicEngine getMappingEngine() {
		return _mappingEngine;
	}

	public synchronized void setMappingEngine(BasicEngine engine) {
		_mappingEngine = engine;
	}

	public synchronized IDb getMappingDb() {
		return _mappingEngine.getMappingDb();
	}

	// public synchronized IDb getUpdateDb() {
	// return _mappingEngine.getUpdateDb();
	// }

	public synchronized void disconnect() throws Exception {
		if (_mappingEngine != null) {
			_mappingEngine.close();
			_mappingEngine = null;
		}

		for (Db db : _recDbs.values()) {
			if (db.isConnected()) {
				db.disconnect();
			}
		}
		_ssf.shutdown();
	}

	public synchronized USDump dump(Peer p) throws DbException {
		/*
		 * if (! getRecMode()) { throw new
		 * DbException("Cannot dump update store when not in rec mode"); }
		 */
		for (Db db : _recDbs.values()) {
			if (db.isConnected()) {
				db.disconnect();
			}
		}

		return _usf.dumpUpdateStore(_mapStore, _schemas.get(p));
	}

	public synchronized void restore(Peer p, USDump dump) throws DbException {
		_usf.restoreUpdateStore(dump);
	}

	public synchronized void reset() throws Exception {
		reset(true);
	}

	public synchronized void reset(boolean replay) throws Exception {
		// if (getRecMode()) {
		for (Db db : _recDbs.values()) {
			db.reset(replay);
			db.disconnect();
		}
		_recDbs.clear();
		// Assume that we have only one schema
		/*
		 * if (_schema == null) { for
		 * (Map<String,edu.upenn.cis.orchestra.datamodel.Schema> schemasForPeer
		 * : _schemas.values()) { for (edu.upenn.cis.orchestra.datamodel.Schema
		 * s : schemasForPeer.values()) { if (_schema == null) { _schema = s; }
		 * else if (! _schema.equals(s)) { throw newDbException(
		 * "Should not be in rec mode if more than one schema is present"); } }
		 * } }
		 */

		// NEW: reset all schemas for all peers
		Set<Schema> allSchemas = new HashSet<Schema>();

		for (Peer p : _peers.values()) {
			allSchemas.addAll(p.getSchemas());
		}

		// for (Schema s : allSchemas)
		// _usf.resetStore(s);// _schema);
		// } else {
		getMappingEngine().reset();
		// }
	}

	static private Element addChild(Document doc, Element parent, String label,
			String name) {
		Element child = doc.createElement(label);
		child.setAttribute("name", name);
		parent.appendChild(child);
		return child;
	}

	public synchronized void serialize(OutputStream out) {
		try {
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element cat = doc.createElement("catalog");
			cat.setAttribute("recmode", Boolean.toString(_recMode));
			cat.setAttribute("name", _name);
			doc.appendChild(cat);
			for (String id : _peers.keySet()) {
				Element peer = addChild(doc, cat, "peer", id);
				_peers.get(id).serialize(doc, peer);
			}
			for (String id : _peers.keySet()) {
				Peer peer = _peers.get(id);
				for (Mapping mapping : peer.getMappings()) {
					Element m = DomUtils.addChild(doc, cat, "mapping");
					mapping.serialize(doc, m);
				}
			}
			if (_mappingEngine != null) {
				Element engine = DomUtils.addChild(doc, cat, "engine");
				_mappingEngine.serialize(doc, engine);
			}
			Element store = DomUtils.addChild(doc, cat, "store");
			Element update = DomUtils.addChild(doc, store, "update");
			Element state = DomUtils.addChild(doc, store, "state");
			if (_usf != null) {
				_usf.serialize(doc, update);
			}
			if (_ssf != null) {
				_ssf.serialize(doc, state);
			}
			for (String peer : _tcs.keySet()) {
				for (String schema : _tcs.get(peer).keySet()) {
					Element trustConds = DomUtils.addChild(doc, cat,
							"trustConditions");
					trustConds.setAttribute("peer", peer);
					trustConds.setAttribute("schema", schema);
					_tcs.get(peer).get(schema).serialize(doc, trustConds,
							_peers.get(peer).getSchema(schema));
				}
			}
			DomUtils.write(doc, out);
		} catch (ParserConfigurationException e) {
			assert (false); // can't happen
		}
	}

	static private Map<String, Schema> deserializeBuiltInFunctions(
			InputStream in) throws XMLParseException {
		Document document = DomUtils.createDocument(in);
		return deserializeBuiltInFunctions(document);
	}

	/**
	 * Returns a mapping from {@code Schema} names to {@code Schema}s of the
	 * built-in functions represented by {@code document}. If {@code db} is
	 * non-null, then each schema found is registered as well.
	 * 
	 * 
	 * @param document
	 * @return a mapping from {@code Schema} names to {@code Schema}s of the
	 *         built-in functions represented by {@code document}
	 * @throws XMLParseException
	 */
	static public Map<String, Schema> deserializeBuiltInFunctions(
			Document document) throws XMLParseException {
		try {
			Element root = document.getDocumentElement();
			if (!root.getNodeName().equals("catalog")) {
				throw new XMLParseException("Missing top-level catalog element");
			}
			List<Element> schemaElements = DomUtils.getChildElementsByName(
					root, "schema");
			Map<String, Schema> builtInSchemas = OrchestraUtil.newHashMap();
			for (Element schemaElement : schemaElements) {
				Schema s = Schema.deserialize(schemaElement, true);
				builtInSchemas.put(s.getSchemaId(), s);
			}
			return builtInSchemas;
		} catch (DuplicateRelationIdException e) {
			throw new XMLParseException("Duplicate relation name "
					+ e.getRelId());
		} catch (UnknownRefFieldException e) {
			throw new XMLParseException("Unknown field in key: " + e);
		} catch (UnsupportedTypeException e) {
			throw new XMLParseException(
					"Unsupported type found while deserializing built-in functions. ",
					e);
		} catch (RelationNotFoundException e) {
			throw new XMLParseException(
					"Relation not found while deserializing built-in functions",
					e);
		}
	}

	/**
	 * Returns an {@code OrchestraSystem} defined by the Orchestra schema file
	 * represented by {@code document}.
	 * 
	 * @param document a {@code Document} representing an Orchestra schema file.
	 * 
	 * @return an {@code OrchestraSystem} defined by the Orchestra schema file
	 *         represented by {@code document}.
	 * 
	 * @throws Exception
	 */
	static public OrchestraSystem deserialize(Document document)
			throws Exception {
		
		return new OrchestraSystem(document);
		/*try {
			System.out.println("*******deserializing catalog********");
			OrchestraSystem catalog = new OrchestraSystem();

			Element root = document.getDocumentElement();
			if (!root.getNodeName().equals("catalog")) {
				throw new XMLParseException("Missing top-level catalog element");
			}
			String catName = root.getAttribute("name");
			if (catName == null || catName.length() == 0) {
				throw new XMLParseException("Catalog must have a name");
			}
			catalog.setName(catName);
			boolean recMode = Boolean
					.parseBoolean(root.getAttribute("recmode"));
			catalog.setRecMode(recMode);

			createStoreFactories(catalog, root);
			ISchemaIDBindingClient client = startStoreServer(catalog);
			PeerFactory peerFactory = new PeerFactory(catName,
					getChildElementsByName(root, "peer"), client);
			List<Peer> peers = peerFactory.retrievePeers();
			catalog._mapStore = peerFactory.getSchemaIDBinding();
			client.disconnect();

			catalog.addPeers(peers);

			NodeList list = root.getChildNodes();

			for (int i = 0; i < list.getLength(); i++) {
				Node node = list.item(i);
				if (node instanceof Element) {
					Element el = (Element) node;
					String name = el.getNodeName();
					if (name.equals("mapping")) {
						Mapping mapping = Mapping.deserialize(catalog, el);
						Peer peer = mapping.getMappingHead().get(0).getPeer();
						peer.addMapping(mapping);
						if (!catalog._bidirectional
								&& mapping.isBidirectional()) {
							catalog._bidirectional = true;
						}
					} else if (name.equals("engine")) {
						InputStream inFile = Config.class
								.getResourceAsStream("functions.schema");
						if (inFile == null) {
							throw new XMLParseException(
									"Cannot find built-in functions");
						}
						Map<String, Schema> builtInSchemas = deserializeBuiltInFunctions(inFile);
						BasicEngine engine = BasicEngine.deserialize(catalog,
								builtInSchemas, el);
						catalog.setMappingEngine(engine);
						catalog._localUpdater = LocalUpdaterFactory
								.newInstance(engine.getMappingDb()
										.getUsername(), engine.getMappingDb()
										.getPassword(), engine.getMappingDb()
										.getServer());
					} else if (el.getNodeName().equals("trustConditions")) {
						if (!el.hasAttribute("peer")
								|| !el.hasAttribute("schema")) {
							throw new XMLParseException(
									"Missing 'peer' or 'schema' attribute", el);
						}
						String peer = el.getAttribute("peer");
						String schemaName = el.getAttribute("schema");
						// Schema s = catalog.getSchemaByName(peer, schemaName);
						// Check for missing peer or schema (will now get null
						// pointer exception)
						// TrustConditions tc = TrustConditions.deserialize(el,
						// s, new StringPeerID(peer));
						TrustConditions tc = TrustConditions.deserialize(el,
								catalog.getPeers(), new StringPeerID(peer));
						Map<String, TrustConditions> tcForPeer = catalog._tcs
								.get(peer);
						if (tcForPeer == null) {
							tcForPeer = new HashMap<String, TrustConditions>();
							catalog._tcs.put(peer, tcForPeer);
						}
						tcForPeer.put(schemaName, tc);
					}
				}
			}
			if (catalog._usf == null || catalog._ssf == null) {
				throw new XMLParseException(
						"Missing <store> element to describe state and update stores");
			}
			return catalog;
		} catch (ParserConfigurationException e) {
			assert (false); // can't happen
		} catch (DuplicatePeerIdException e) {
			throw new XMLParseException("Duplicate peer name " + e.getPeerId());
		} catch (DuplicateSchemaIdException e) {
			throw new XMLParseException("Duplicate schema name "
					+ e.getSchemaId());
		} catch (DuplicateRelationIdException e) {
			throw new XMLParseException("Duplicate relation name "
					+ e.getRelId());
		} catch (UnknownRefFieldException e) {
			throw new XMLParseException("Unknown field in key: " + e);
		} catch (DuplicateMappingIdException e) {
			throw new XMLParseException("Duplicate mapping name "
					+ e.getMappingId());
		}
		return null;*/
	}

	/**
	 * Returns an {@code OrchestraSystem} defined by the Orchestra schema file
	 * represented by {@code document}.
	 * 
	 * @param document a {@code Document} representing an Orchestra schema file.

	 * @throws Exception
	 */
	public OrchestraSystem(Document document) throws Exception {
		this(document, null);
	}

	/**
	 * Returns an {@code OrchestraSystem} defined by the Orchestra schema file
	 * represented by {@code document}.
	 * 
	 * @param document a {@code Document} representing an Orchestra schema file.
	 * @param updateStoreFactory 
	 * 
	 * 
	 * @throws Exception
	 */
	public OrchestraSystem(Document document, UpdateStore.Factory updateStoreFactory) throws Exception {
		this();
		try {
			System.out.println("*******deserializing catalog********");

			Element catalogElement = document.getDocumentElement();
			createPeers(catalogElement, updateStoreFactory);

			NodeList list = catalogElement.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				Node node = list.item(i);
				if (node instanceof Element) {
					Element el = (Element) node;
					String name = el.getNodeName();
					if (name.equals("mapping")) {
						Mapping mapping = Mapping.deserialize(this, el);
						Peer peer = mapping.getMappingHead().get(0).getPeer();
						peer.addMapping(mapping);
						if (!_bidirectional && mapping.isBidirectional()) {
							_bidirectional = true;
						}
					} else if (el.getNodeName().equals("trustConditions")) {
						if (!el.hasAttribute("peer")
								|| !el.hasAttribute("schema")) {
							throw new XMLParseException(
									"Missing 'peer' or 'schema' attribute", el);
						}
						String peer = el.getAttribute("peer");
						String schemaName = el.getAttribute("schema");
						TrustConditions tc = TrustConditions.deserialize(el,
								getPeers(), new StringPeerID(peer));
						Map<String, TrustConditions> tcForPeer = _tcs.get(peer);
						if (tcForPeer == null) {
							tcForPeer = new HashMap<String, TrustConditions>();
							_tcs.put(peer, tcForPeer);
						}
						tcForPeer.put(schemaName, tc);
					}
				}
			}
			// Engine creation must come after mapping creation.
			Element engineElement = getChildElementByName(catalogElement,
			"engine");
			createEngine(engineElement);
			if (_usf == null || _ssf == null) {
				throw new XMLParseException(
						"Missing <store> element to describe state and update stores");
			}
		} catch (ParserConfigurationException e) {
			assert (false); // can't happen
		} catch (DuplicatePeerIdException e) {
			throw new XMLParseException("Duplicate peer name " + e.getPeerId());
		} catch (DuplicateSchemaIdException e) {
			throw new XMLParseException("Duplicate schema name "
					+ e.getSchemaId());
		} catch (DuplicateRelationIdException e) {
			throw new XMLParseException("Duplicate relation name "
					+ e.getRelId());
		} catch (UnknownRefFieldException e) {
			throw new XMLParseException("Unknown field in key: " + e);
		} catch (DuplicateMappingIdException e) {
			throw new XMLParseException("Duplicate mapping name "
					+ e.getMappingId());
		}
	}

	/**
	 * Given and "engine" element, creates and sets the mapping {@code
	 * BasicEngine} for this {@code OrchestraSystem}.
	 * 
	 * @param engineElement
	 * @throws XMLParseException
	 * @throws Exception
	 * @throws NoLocalUpdaterClassException
	 * @throws NoExtractorClassException
	 */
	private void createEngine(Element engineElement)
			throws XMLParseException, Exception, NoLocalUpdaterClassException,
			NoExtractorClassException {
		InputStream inFile = Config.class
				.getResourceAsStream("functions.schema");
		if (inFile == null) {
			throw new XMLParseException("Cannot find built-in functions");
		}
		Map<String, Schema> builtInSchemas = deserializeBuiltInFunctions(inFile);
		BasicEngine engine = BasicEngine.deserialize(this, builtInSchemas,
				engineElement);
		setMappingEngine(engine);
		_localUpdater = LocalUpdaterFactory.newInstance(engine.getMappingDb()
				.getUsername(), engine.getMappingDb().getPassword(), engine
				.getMappingDb().getServer());
	}

	/**
	 * Given a "catalog" {@code Element} creates and sets the peers for this
	 * {@code OrchestraSystem}.
	 * 
	 * @param catalogElement
	 * @throws XMLParseException
	 * @throws Exception
	 * @throws USException
	 * @throws DuplicateSchemaIdException
	 * @throws DuplicatePeerIdException
	 * @throws NoLocalPeerException
	 */
	private void createPeers(Element catalogElement, UpdateStore.Factory updateStoreFactory) throws XMLParseException,
			Exception, USException, DuplicateSchemaIdException,
			DuplicatePeerIdException, NoLocalPeerException {
		if (!catalogElement.getNodeName().equals("catalog")) {
			throw new XMLParseException("Missing top-level catalog element");
		}
		String catName = catalogElement.getAttribute("name");
		if (catName == null || catName.length() == 0) {
			throw new XMLParseException("Catalog must have a name");
		}
		setName(catName);
		boolean recMode = Boolean.parseBoolean(catalogElement
				.getAttribute("recmode"));
		setRecMode(recMode);

		createStoreFactories(catalogElement, updateStoreFactory);
		ISchemaIDBindingClient client = startUpdateStoreServer();
		PeerFactory peerFactory = new PeerFactory(catName,
				getChildElementsByName(catalogElement, "peer"), client);
		List<Peer> peers = peerFactory.retrievePeers();
		_mapStore = peerFactory.getSchemaIDBinding();
		client.disconnect();

		addPeers(peers);
	}

	/**
	 * Deserialize store information contained in the {@code store} child of
	 * {@code root}. If {@code updateStoreFactory} is non-null, then it is used
	 * and the "update" Element is ignored.
	 * 
	 * @param root
	 * @param updateStoreFactory
	 * 
	 */
	private void createStoreFactories(Element root,
			UpdateStore.Factory updateStoreFactory) throws XMLParseException {
		// Create store factories
		Element store = getChildElementByName(root, "store");

		Element state = DomUtils.getChildElementByName(store, "state");
		_ssf = StateStore.deserialize(state);

		if (updateStoreFactory == null) {
			Element update = DomUtils.getChildElementByName(store, "update");
			if (update == null || state == null) {
				throw new XMLParseException("Missing <update> or <state> tag",
						store);
			}
			_usf = UpdateStore.deserialize(update);
		} else {
			_usf = updateStoreFactory;
		}

	}

	/**
	 * Parses and unfolds a query and returns a list of rules with provenance
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Rule> unfoldQuery(BufferedReader source) throws Exception {
		String next = source.readLine();
		Map<String, RelationContext> localDefs = new HashMap<String, RelationContext>();

		List<Rule> rules = new ArrayList<Rule>();
		while (next != null) {
			// Loop and build a DatalogSequence
			System.out.println("String:" + next.toString());
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else if (next.startsWith("*")) {
				next = next.substring(1, next.length());
				throw new RecursionException("Found *");
			} else if (next.endsWith(".")) {
				next = next.substring(0, next.length() - 1);
			} else {
				rules.add(Rule.parse(this, next, localDefs));
			}
			next = source.readLine();
		}

		Map<Atom, ProvenanceNode> prov = new HashMap<Atom, ProvenanceNode>();

		Set<String> provenanceRelations = new HashSet<String>();
		// Assume the last rule is the distinguished variable
		return DatalogViewUnfolder.unfoldQuery(rules, rules.get(
				rules.size() - 1).getHead().getRelationContext().toString(),
				prov, provenanceRelations, "", "", true);
	}

	/**
	 * Parses, unfolds, and executes a query and returns a list of tuples with
	 * provenance
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Tuple> runUnfoldedQuery(BufferedReader source,
			boolean provenanceQuery) throws Exception {
		List<Rule> rules = unfoldQuery(source);

		return runUnfoldedQuery(rules, false, "", true, provenanceQuery);
	}

	public List<Tuple> runUnfoldedQuery(List<Rule> rules, boolean provenance,
			String semiringName, boolean returnResults, boolean provenanceQuery)
			throws Exception {
		List<Tuple> results = new ArrayList<Tuple>();
		BasicEngine eng = getMappingEngine();

		if (!provenance) {
			long time = 0;
			long timeRes = 0;

			for (Rule r : rules) {
				Calendar before = Calendar.getInstance();
				ResultSetIterator<Tuple> result = eng.evalQueryRule(r);
				Calendar after = Calendar.getInstance();
				long oneTime = after.getTimeInMillis()
						- before.getTimeInMillis();
				System.out.println("EXP: LAST PROVENANCE QUERY: " + oneTime
						+ " msec");
				time += after.getTimeInMillis() - before.getTimeInMillis();

				if (returnResults) {
					while (result != null && result.hasNext()) {
						Tuple tuple = result.next();
						tuple.setOrigin(r.getHead().getRelationContext());
						results.add(tuple);
					}
					result.close();
				}
				Calendar afterRes = Calendar.getInstance();
				timeRes += afterRes.getTimeInMillis()
						- before.getTimeInMillis();
			}
			System.out.println("PROQL EXP: NET PROQL EVAL TIME: " + time
					+ " msec");
			System.out
					.println("PROQL EXP: TOTAL PROQL EVAL TIME (INCL RESULT CONSTR): "
							+ timeRes + " msec");
		} else {
			long time = 0;
			long timeRes = 0;
			Calendar before = Calendar.getInstance();
			List<ResultSetIterator<Tuple>> res = eng.evalRuleSet(rules,
					semiringName, provenanceQuery);
			Calendar after = Calendar.getInstance();
			time += after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("PROQL EXP: NET EVAL TIME: " + time + " msec");

			for (ResultSetIterator<Tuple> result : res) {
				if (returnResults) {
					int i = 0;
					while (result != null && result.hasNext()) {
						Tuple tuple = result.next();
						tuple.setOrigin(rules.get(0).getHead()
								.getRelationContext());
						results.add(tuple);
						i++;
					}
					System.out.println("Query returned " + i + " results");
				}
				result.close();
			}
			Calendar afterRes = Calendar.getInstance();
			timeRes += afterRes.getTimeInMillis() - before.getTimeInMillis();

			System.out
					.println("PROQL EXP: TOTAL EVAL TIME (INCL RESULT CONSTR): "
							+ timeRes + " msec");
		}
		return results;

	}

	/**
	 * Parses and runs a potentially-recursive query
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Tuple> runMaterializedQuery(BufferedReader source)
			throws Exception {
		List<Tuple> results = new ArrayList<Tuple>();

		String next = source.readLine();
		List<Datalog> progs = new ArrayList<Datalog>();
		List<Rule> rules = new ArrayList<Rule>();

		Rule lastRule = null;

		boolean addLastProgram = false;
		boolean lastWasRecursive = false;
		boolean c4f = true;
		while (next != null) {
			// Loop and build a DatalogSequence
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else {
				boolean hasPeriod = false;
				boolean isRecursive = false;
				c4f = true;

				if (next.startsWith("-")) {
					c4f = false;
					next = next.substring(1, next.length());
				}
				if (next.startsWith("*")) {
					isRecursive = true;
					// lastWasRecursive = true;
					next = next.substring(1, next.length());
				}
				if (next.endsWith(".")) {
					hasPeriod = true;
					next = next.substring(0, next.length() - 1);
				}

				// Single recursive rule: gets its own program
				if (isRecursive) {
					// Close the last program
					if (addLastProgram) {
						if (lastWasRecursive) {
							DatalogProgram prog = new RecursiveDatalogProgram(
									rules, c4f);
							progs.add(prog);
							// lastWasRecursive = false;
						} else {
							DatalogProgram prog = new NonRecursiveDatalogProgram(
									rules, c4f);// isMigrated());
							progs.add(prog);
						}
					}

					// Create an independent program
					rules = new ArrayList<Rule>();
					Rule r = Rule.parse(this, next);
					lastRule = r;
					rules.add(r);

					// Don't count for fixpoint
					DatalogProgram prog = new RecursiveDatalogProgram(rules,
							c4f);
					progs.add(prog);

					// Create a next program for whatever rules are next
					rules = new ArrayList<Rule>();
					addLastProgram = false;

					// Period: end of a program / stratum
				} else if (hasPeriod) {
					DatalogProgram prog = new RecursiveDatalogProgram(rules,
							c4f);// isMigrated());
					progs.add(prog);
					Rule r = Rule.parse(this, next);
					lastRule = r;
					rules.add(r);
					rules = new ArrayList<Rule>();
					addLastProgram = false;

				} else {
					Rule r = Rule.parse(this, next);
					rules.add(r);
					lastRule = r;
					addLastProgram = true;
					lastWasRecursive = false;
				}
			}
			next = source.readLine();
		}
		// In case there was no terminating period
		if (addLastProgram) {
			// NonRecursiveDatalogProgram prog = new
			// NonRecursiveDatalogProgram(rules, isMigrated());
			// RecursiveDatalogProgram prog = new RecursiveDatalogProgram(rules,
			// isMigrated());
			// progs.add(prog);
			if (lastWasRecursive) {
				DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);// isMigrated());
				progs.add(prog);
				lastWasRecursive = false;
			} else {
				DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);// isMigrated());
				progs.add(prog);
			}
		}
		source.close();

		if (!getMappingDb().isConnected())
			getMappingDb().connect();
		DatalogSequence cur = new DatalogSequence(true, progs, false);
		final DatalogEngine de = new DatalogEngine(getMappingDb());

		while (de.evaluatePrograms(cur) > 0)
			;

		List<Atom> body = new ArrayList<Atom>();
		body.add(lastRule.getHead());
		Rule resultQuery = new Rule(lastRule.getHead(), body, null, lastRule
				.getBuiltInSchemas());

		BasicEngine eng = getMappingEngine();
		ResultSetIterator<Tuple> result = eng.evalQueryRule(resultQuery);

		while (result != null && result.hasNext()) {
			Tuple tuple = result.next();
			results.add(tuple);
		}
		result.close();
		return results;
	}

	public void runMaterializedQuery(String filename) throws Exception {
		BufferedReader source = new BufferedReader(new FileReader(filename));

		runMaterializedQuery(source);
	}

	public List<Datalog> runP2PQuery(String filename) throws Exception {
		BufferedReader source = new BufferedReader(new FileReader(filename));
		String next = source.readLine();
		List<Datalog> progs = new ArrayList<Datalog>();
		List<Rule> rules = new ArrayList<Rule>();
		boolean addLastProgram = false;
		boolean lastWasRecursive = false;
		boolean c4f = true;
		while (next != null) {
			// Loop and build a DatalogSequence
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else {
				boolean hasPeriod = false;
				boolean isRecursive = false;
				c4f = true;

				if (next.startsWith("-")) {
					c4f = false;
					next = next.substring(1, next.length());
				}
				if (next.startsWith("*")) {
					isRecursive = true;
					// lastWasRecursive = true;
					next = next.substring(1, next.length());
				}
				if (next.endsWith(".")) {
					hasPeriod = true;
					next = next.substring(0, next.length() - 1);
				}

				// Single recursive rule: gets its own program
				if (isRecursive) {
					// Close the last program
					if (addLastProgram) {
						if (lastWasRecursive) {
							DatalogProgram prog = new RecursiveDatalogProgram(
									rules, c4f);
							progs.add(prog);
							// lastWasRecursive = false;
						} else {
							DatalogProgram prog = new NonRecursiveDatalogProgram(
									rules, c4f);// isMigrated());
							progs.add(prog);
						}
					}

					// Create an independent program
					rules = new ArrayList<Rule>();
					Rule r = Rule.parse(this, next);
					rules.add(r);

					// Don't count for fixpoint
					DatalogProgram prog = new RecursiveDatalogProgram(rules,
							c4f);
					progs.add(prog);

					// Create a next program for whatever rules are next
					rules = new ArrayList<Rule>();
					addLastProgram = false;

					// Period: end of a program / stratum
				} else if (hasPeriod) {
					DatalogProgram prog = new RecursiveDatalogProgram(rules,
							c4f);// isMigrated());
					progs.add(prog);
					Rule r = Rule.parse(this, next);
					rules.add(r);
					rules = new ArrayList<Rule>();
					addLastProgram = false;

				} else {
					Rule r = Rule.parse(this, next);
					rules.add(r);
					addLastProgram = true;
					lastWasRecursive = false;
				}
			}
			next = source.readLine();
		}
		// In case there was no terminating period
		if (addLastProgram) {
			if (lastWasRecursive) {
				DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);// isMigrated());
				progs.add(prog);
				lastWasRecursive = false;
			} else {
				DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);// isMigrated());
				progs.add(prog);
			}
		}
		source.close();
		return progs;
	}

	/**
	 * This method is a no-op. The store server is started as part of the
	 * deserialization of this {@code OrchestraSystem}.
	 * 
	 * @throws Exception
	 */
	@Deprecated
	public void startStoreServer() throws Exception {

	}

	private ISchemaIDBindingClient startUpdateStoreServer() throws Exception {
		ISchemaIDBindingClient schemaIDBindingClient = _usf
				.getSchemaIDBindingClient();
		boolean connected = schemaIDBindingClient.reconnect();
		if (!connected) {
			Debug
					.println("Cannot connect to update store. Checking to see if update store should live here.");
			_usf.startUpdateStoreServer();
			long currentTime = System.currentTimeMillis();
			long stopTime = currentTime + 10000;
			for (; currentTime < stopTime && !connected; currentTime = System
					.currentTimeMillis()) {
				_logger
						.debug("Failed to connect to update store. Trying again.");
				connected = schemaIDBindingClient.reconnect();

				Thread.sleep(100);
			}
			if (!connected) {
				throw new USException("Cannot connect to the update store");
			}
		}
		return schemaIDBindingClient;
	}

	/**
	 * This method is a no-op. The store server is started as part of the
	 * deserialization of this {@code OrchestraSystem}.
	 * 
	 * @throws Exception
	 */
	@Deprecated
	public void stopStoreServer() throws Exception {}

	/**
	 * Returns {@code true} if the update store is running, and {@code false}
	 * otherwise.
	 * 
	 * @return {@code true} if the update store is running, and {@code false}
	 *         otherwise.
	 */
	public boolean storeServerRunning() {
		return _usf.updateStoreServerIsRunning();
	}

	/**
	 * Deletes all records from the store server.
	 * 
	 * @throws IllegalStateException
	 */
	public void clearStoreServer() throws IllegalStateException {
		try {
			_usf.resetStore(null);
		} catch (USException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Update exchange for the peer {@code peer}.
	 * 
	 * @throws Exception
	 */
	public void translate() throws Exception {
		_logger.debug("Starting update exchange.");
		String localPeerId = _localPeer.getId();
		int lastrec = getRecDb(localPeerId).getCurrentRecno();
		int recno = getRecDb(localPeerId).getRecNo();
		BasicEngine engine = getMappingEngine();
		engine.mapUpdates(lastrec, recno, _localPeer, false);
		getRecDb(localPeerId).setRecDone();
		List<Relation> relations = getLocalPeerRelations();
		_localUpdater.postReconcileHook(getMappingDb(), relations);
		_logger.debug("Update exchange finished.");
	}

	/**
	 * Reconcile {@code peer}.
	 * 
	 * @throws DbException
	 */
	public void reconcile() throws DbException {
		getRecDb(_localPeer.getId()).reconcile();
		List<Relation> relations = getLocalPeerRelations();
		_localUpdater.postReconcileHook(getMappingDb(), relations);
	}

	/**
	 * Publish the local peer's updates.
	 * 
	 * @return the number of update transactions published.
	 * @throws Exception
	 */
	public int fetch() throws Exception {
		_logger.debug("Starting publish.");
		_localUpdater.extractAndApplyLocalUpdates(_localPeer);
		int count = getMappingDb().fetchDbTransactions(_localPeer,
				getRecDb(_localPeer.getId()));
		getRecDb(_localPeer.getId()).publish();
		_logger.debug("Publish finished.");
		return count;
	}

	/**
	 * Publishes the local peer's updates and then performs an update exchange
	 * mapping.
	 * 
	 * @return the number of update transactions published
	 * @throws Exception
	 */
	public int publishAndMap() throws Exception {
		int transactions = fetch();
		if (getRecMode()) {
			reconcile();
		} else {
			if (getMappingDb().isConnected()) {
				getMappingDb().connect();
			}
			// Now run the Exchange
			translate();

		}
		return transactions;
	}

	/**
	 * Imports data into the local peers instance.
	 * 
	 * @param dir a directory containing data files
	 * @param succeeded a list for holding success messages
	 * @param failed a list for holding failure messages
	 * @throws IOException
	 */
	public void importUpdates(String dir, ArrayList<String> succeeded,
			ArrayList<String> failed) throws IOException {
		getMappingEngine().importUpdates(_localPeer, dir, succeeded, failed);
	}

	/**
	 * Returns an {@code OrchestraSystem} defined by the Orchestra schema file
	 * represented by {@code in}.
	 * 
	 * @param in an {@code InputStream} representing an Orchestra schema file.
	 * 
	 * @return an {@code OrchestraSystem} defined by the Orchestra schema file
	 *         represented by {@code in}.
	 * 
	 * @throws Exception
	 */
	public static OrchestraSystem deserialize(InputStream in) throws Exception {
		Document document = createDocument(in);

		return deserialize(document);
	}

	/**
	 * Returns {@code true} if this {@code OrchestraSystem} contains
	 * bidirectional mappings, otherwise {@code false}.
	 * 
	 * @return {@code true} if this {@code OrchestraSystem} contains
	 *         bidirectional mappings, otherwise {@code false}
	 */
	public boolean isBidirectional() {
		return _bidirectional;
	}

	/**
	 * Returns {@code true} if {@code peer} is this {@code OrchestraSystem}'s
	 * local peer, and {@code false} otherwise.
	 * 
	 * @param peer the {@code Peer} to be tested
	 * @return {@code true} if {@code peer} is this {@code OrchestraSystem}'s
	 *         local peer
	 */
	public boolean isLocalPeer(Peer peer) {
		return _localPeer.equals(peer);
	}

	/**
	 * Called once during Migrate to allow the {@code ILocalUpdater} to perform
	 * any initial setup.
	 * 
	 */
	public void prepareSystemForLocalUpdater() {
		List<Relation> relations = getLocalPeerRelations();
		_localUpdater.prepare(getMappingDb(), relations);
	}

	private List<Relation> getLocalPeerRelations() {
		List<Relation> relations = newArrayList();
		for (Schema schema : _localPeer.getSchemas()) {
			for (Relation relation : schema.getRelations()) {
				if (!relation.isInternalRelation()) {
					relations.add(relation);
				}
			}
		}
		return relations;
	}

}
