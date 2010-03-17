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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringPeerID;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Persistent binding between Schema objects and integer IDs
 * 
 * @author zives
 *
 */
public class SchemaIDBinding implements ISchemaIDBinding {
	
	/**
	 * BerkeleyDB store for peer & schema info
	 */
	private Database _peerSchemaInfo;
	private DatabaseConfig _schemaInfoDc;
	private Environment _env = null;
	
	/**
	 * Next integer ID to assign
	 */
	private static int _nextId = 1;
	
	public static synchronized int getRelationId() {
		return _nextId++;
	}
	
	private Map<String,SchemaMap> _cdssSchema;
	
	/**
	 * Create a new binding store, using the BerkeleyDB environment
	 * @param env
	 * @throws DatabaseException
	 */
	public SchemaIDBinding(Environment env) throws DatabaseException {
		_schemaInfoDc = new DatabaseConfig();
		EnvironmentConfig config = env.getConfig();
		if (config.getReadOnly()){
			_schemaInfoDc.setReadOnly(true);
		}
		_schemaInfoDc.setAllowCreate(true);
		_schemaInfoDc.setSortedDuplicates(false);
		_schemaInfoDc.setTransactional(false);

		_peerSchemaInfo = env.openDatabase(null, "schemaInfo", _schemaInfoDc);
		_env = env;

		_cdssSchema = new HashMap<String,SchemaMap>();
		
		loadMap();
	}

	/**
	 * Initializes the cdss to SchemaMap map with what is on disk.
	 * 
	 * @throws DatabaseException
	 */
	private void loadMap() throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		Cursor c = _peerSchemaInfo.openCursor(null, null);
		try {
			while (c.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				// Decode key
				ByteBufferReader keyReader = new ByteBufferReader(key.getData());
				String cdss = keyReader.readString();
				// Decode data
				ByteArrayInputStream schMap = new ByteArrayInputStream(data
						.getData());
				ObjectInputStream os = new ObjectInputStream(schMap);
				SchemaMap map = (SchemaMap) os.readObject();
				_cdssSchema.put(cdss, map);
				int val = map.getMaxRelationID() + 1;
				if (_nextId < val) {
					_nextId = val;
				}
			}
		} catch (IOException e) {
			throw new DatabaseException(e);
		} catch (ClassNotFoundException e) {
			throw new DatabaseException(e);
		} finally {
			c.close();
		}
	}

	/**
	 * Reset the database and the counter
	 * 
	 * @param env
	 * @throws DatabaseException
	 */
	public void clear(Environment env) throws DatabaseException {
		_peerSchemaInfo.close();
		_env = env;
		_env.truncateDatabase(null, "schemaInfo", false);
		_peerSchemaInfo = _env.openDatabase(null, "schemaInfo", _schemaInfoDc);
		_cdssSchema = new HashMap<String,SchemaMap>();
		
		_nextId = 1;
	}
	
	/**
	 * Returns the relation with the requested ID
	 * @param relCode
	 * @return
	 */
	@Override
	public Relation getRelationFor(int relCode) {
		for (String cdss : _cdssSchema.keySet()) {
			Map<Schema,Map<Relation,Integer>> m = _cdssSchema.get(cdss).getRelationIdMap();
			
			for (Schema s: m.keySet()) {
				for (Relation r: m.get(s).keySet()) {
					if (r.getRelationID() == relCode)
						return r;
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Returns the relation with the specific name.  Scans
	 * different CDSSs in arbitrary order, so not recommended if
	 * we have the same relation in multiple schemas
	 *
	 * @deprecated
	 * @param nam
	 * @return
	 */
	@Override
	public Relation getRelationNamed(String nam) {
		for (String cdss : _cdssSchema.keySet()) {
			Map<Schema,Map<Relation,Integer>> m = _cdssSchema.get(cdss).getRelationIdMap();
			
			for (Schema s: m.keySet()) {
				for (Relation r: m.get(s).keySet()) {
					if (r.getName().equals(nam))
						return r;
				}
			}
		}
		
		return null;
	}

	/**
	 * Returns the schema for the named peer
	 * 
	 * @param pid
	 * @return
	 */
	@Override
	public Schema getSchema(AbstractPeerID pid) throws USException {
		for (String cdss : _cdssSchema.keySet()) {
			Schema s = _cdssSchema.get(cdss).getSchemaForPeer(pid);
			if (s != null)
				return s;
		}
		throw new USException("Cannot find schema for peer " + pid);
		/*
		synchronized (schemas) {
			Schema s = schemas.get(pid);
			if (s == null) {
				throw new USException("Cannot find schema for peer " + pid + " in list " + schemas.keySet().toString());
			} else {
				return s;
			}
		}*/
	}
	
	/**
	 * Returns the schema for the named system and peer
	 *
	 * @param cdss 
	 * @param pid
	 * @return the schema for the named system and peer
	 */
	public Schema getSchema(String cdss, AbstractPeerID pid) {
		SchemaMap map = _cdssSchema.get(cdss);
		Schema s = null;
		if (map != null) {
			s = map.getSchemaForPeer(pid);
		}
		return s;
	}

	/**
	 * Shuts down the storage
	 */
	public void quit() {
		try {
			_peerSchemaInfo.close();
		} catch (DatabaseException d) {
			
		}
	}
	

	/**
	 * For a given CDSS namespace, register all of the schemas and bind the PeerID
	 * to each schema, plus an int ID for each relation
	 * 
	 * @param namespace
	 * @param schemas
	 * @param peerSchema
	 * @return
	 */
	public Map<Schema,Map<Relation,Integer>> registerAllSchemas(String namespace, List<Schema> schemas, 
			Map<AbstractPeerID,Integer> peerSchema) {
		SchemaMap map = _cdssSchema.get(namespace);
		
		boolean save = true;
		if (map == null) {
			try {
				map = getSchemaMap(namespace);
				
				if (map != null) {
					int val = map.getMaxRelationID() + 1;
					if (_nextId < val)
						_nextId = val;
				}
				if (map == null) {
					System.out.println("Unable to find schema map " + namespace);
					map = new SchemaMap();
					map.addSchemas(schemas, peerSchema);
				} else {
					System.out.println("Successfully loaded schema map " + namespace);
					save = false;
				}
			} catch (IOException e) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			} catch (ClassNotFoundException f) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			} catch (DatabaseException d) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			}
		}
		_cdssSchema.put(namespace, map);
		
		if (save) {
			boolean succ = false;
			try {
				succ = saveSchemaMap(namespace);
				
				//This sync seems to do a better job of writing schemaMap to disk than closing DB.
				//Maybe unnecessary if someone calls quit on bdbserver.
				_env.sync();
				//_peerSchemaInfo.close();
				//_peerSchemaInfo = _env.openDatabase(null, "schemaInfo", _schemaInfoDc);
			} catch (IOException e) {
				System.err.println("Unable to save schemas for " + namespace);
				e.printStackTrace();
			} catch (DatabaseException d) {
				System.err.println("Unable to save schemas for " + namespace);
				d.printStackTrace();
			}
			
			if (succ)
				Debug.println("Successfully saved schema map: " + map.getRelationIdMap());
			else
				System.err.println("Unable to save schema map");
		}
		
		return map.getRelationIdMap();
	}


	/**
	 * Returns the SchemaMap for the requested CDSS namespace
	 * 
	 * @param namespace
	 * @return
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws ClassNotFoundException
	 */
	public SchemaMap getSchemaMap(String namespace) throws IOException, DatabaseException,
	ClassNotFoundException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(namespace);
	
		DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());
	
		DatabaseEntry data = new DatabaseEntry();
		
		OperationStatus os2 = _peerSchemaInfo.get(null, key, data, LockMode.DEFAULT);
		
		if (os2 == OperationStatus.SUCCESS) {
			ByteArrayInputStream schMap = new ByteArrayInputStream(data.getData());
		
			ObjectInputStream os = new ObjectInputStream(schMap);
		
			SchemaMap map = (SchemaMap)os.readObject();
			return map;
		} else
			return null;
	}

	/**
	 * Adds a new relation and schema to the namespace
	 * 
	 * @param namespace
	 * @param schema
	 * @param rel
	 * @return
	 */
	public int registerNewRelation(String namespace, Schema schema, Relation rel) {
		SchemaMap map = _cdssSchema.get(namespace);
	
		map.addMappingforSchema(schema);
		
		return map.getIdforRelation(schema, rel);
	}

	/**
	 * Saves the Schema ID Binding to disk
	 * 
	 * @param namespace
	 * @return
	 * @throws IOException
	 * @throws DatabaseException
	 */
	private boolean saveSchemaMap(String namespace) throws IOException, DatabaseException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(namespace);
	
		DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());
		
		ByteArrayOutputStream schMap = new ByteArrayOutputStream();
		
		SchemaMap map = _cdssSchema.get(namespace);
		
		ObjectOutputStream os = new ObjectOutputStream(schMap);
		
		os.writeObject(map);
		
		os.flush();
	
		DatabaseEntry data = new DatabaseEntry(schMap.toByteArray());
		
		OperationStatus os2 = _peerSchemaInfo.put(null, key, data);
		
		try {
			if (os2 == OperationStatus.SUCCESS) {
				SchemaMap ret = getSchemaMap(namespace);
				
				if (ret != null)
					System.out.println("Successfully verified schema " + namespace);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return (os2 == OperationStatus.SUCCESS);
	}

	/**
	 * Returns the result of deserializing the passed in document containing a
	 * CDSS definition.
	 * <p>
	 * Root element name of {@code peerDocument} should be the name of the CDSS
	 * being defined. The child elements are {@code peer} elements as in {@code
	 * .schema} files.
	 * 
	 * @param peerDocument
	 * @return the result of deserializing the passed in document containing a
	 *         CDSS definition
	 * @throws DuplicateRelationIdException
	 * @throws UnknownRefFieldException
	 * @throws XMLParseException
	 * @throws UnsupportedTypeException
	 * @throws RelationNotFoundException
	 */
	public Map<AbstractPeerID, Schema> loadSchemas(Document peerDocument)
			throws DuplicateRelationIdException, UnknownRefFieldException,
			XMLParseException, UnsupportedTypeException,
			RelationNotFoundException {
		Element root = peerDocument.getDocumentElement();
		String cdss = root.getNodeName();
		List<Element> peers = getChildElementsByName(root, "peer");
		Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap();
		List<Schema> schemas = newArrayList();
		Map<AbstractPeerID, Integer> peerIDToInteger = newHashMap();
		int i = 0;
		for (Element peer : peers) {
			AbstractPeerID pid = new StringPeerID(peer.getAttribute("name"));
			Element schemaElement = getChildElementByName(peer, "schema");
			Schema schema = Schema.deserialize(schemaElement, false);
			peerIDToSchema.put(pid, schema);
			schemas.add(schema);
			peerIDToInteger.put(pid, Integer.valueOf(i++));
		}
		registerAllSchemas(cdss, schemas, peerIDToInteger);
		return peerIDToSchema;

	}

	/**
	 * Mapping between Peers, Schemas, Relations, and int IDs for the relations
	 * 
	 * @author zives
	 *
	 */
	static class SchemaMap implements Serializable {
		public SchemaMap() {
			_schemaRelationIdMap = new HashMap<Schema,Map<Relation,Integer>>();
			_peerSchemaMap = new HashMap<AbstractPeerID,Schema>();
		}
		
		public Map<Schema,Map<Relation,Integer>> getRelationIdMap() {
			return _schemaRelationIdMap;
		}
		
		public void addSchemas(List<Schema> schemas, Map<AbstractPeerID,Integer> peers){
			for (AbstractPeerID p : peers.keySet())
				_peerSchemaMap.put(p, schemas.get(peers.get(p).intValue()));
			
			for (Schema s: schemas)
				addMappingforSchema(s);
		}
		
		public int getMaxRelationID() {
			int ret = Integer.MIN_VALUE;
			for (Schema s: _schemaRelationIdMap.keySet())
				for (Integer i : _schemaRelationIdMap.get(s).values())
					if (i > ret)
						ret = i;
			
			return ret;
		}
		
		/**
		 * Takes a given schema and adds an int mapping for each relation
		 * @param s
		 */
		public void addMappingforSchema(Schema s) {
			Map<Relation,Integer> relMap;
			
			synchronized (_schemaRelationIdMap) {
				relMap = _schemaRelationIdMap.get(s); 
				if (relMap == null) {
					relMap = new HashMap<Relation,Integer>();
					_schemaRelationIdMap.put(s, relMap);
				}
				for (Relation r: s.getRelations()) {
					if (!relMap.containsKey(r))
						relMap.put(r, r.getRelationID());// new Integer(getRelationId()));
					
					r.setRelationID(relMap.get(r));
				}
			}
			s.resetIDs();
		}
		
		public int getIdforRelation(Schema s, Relation r) {
			synchronized(_schemaRelationIdMap) {
				return _schemaRelationIdMap.get(s).get(r).intValue();
			}
		}
		
		public Schema getSchemaForPeer(AbstractPeerID p) {
			synchronized (_peerSchemaMap) {
				return _peerSchemaMap.get(p);
			}
		}
		
		public void addPeerSchema(AbstractPeerID p, Schema s) {
			synchronized (_peerSchemaMap) {
				_peerSchemaMap.put(p, s);
			}
		}
		
		Map<Schema,Map<Relation,Integer>> _schemaRelationIdMap;
		Map<AbstractPeerID,Schema> _peerSchemaMap;
		
		public static final long serialVersionUID = 1;
	}
	
	public static void resetRelationId() {
		_nextId = 1;
	}
	
	private Set<AbstractPeerID> getPeersForNamespace(String cdss)
			throws IOException, DatabaseException, ClassNotFoundException {
		SchemaMap map = _cdssSchema.get(cdss);
		Set<AbstractPeerID> result;
		if (map == null) {
			result = Collections.emptySet();
		} else {
			result = map._peerSchemaMap.keySet();
		}
		return result;
	}
	
	public Map<AbstractPeerID, Schema> getSchemasForNamespace(String cdss)
			throws IOException, DatabaseException, ClassNotFoundException {
		SchemaMap map = _cdssSchema.get(cdss);
		Map<AbstractPeerID, Schema> result;
		if (map == null) {
			result = Collections.emptyMap();
		} else {
			result = map._peerSchemaMap;
		}
		return result;
	}

	/**
	 * DOCUMENT ME
	 * 
	 * @return
	 */
	public Set<String> getSystems() {
		return _cdssSchema.keySet();
	}
}