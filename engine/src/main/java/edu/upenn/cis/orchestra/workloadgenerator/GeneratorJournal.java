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
package edu.upenn.cis.orchestra.workloadgenerator;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

/**
 * A journal for logging what changes we make as we evolve an Orchestra schema.
 * 
 * The operations are <code>"addPeer"</code>, <code>"deletePeer"</code>,
 * <code>"addBypass"</code>, and <code>"deleteBypass</code>.
 * 
 * @author Sam Donnelly
 */
public class GeneratorJournal {

	/**
	 * Peers that have been added through the schema changes.
	 */
	private List<Integer> _peers = new ArrayList<Integer>();

	/**
	 * Stores the logical schemas that have been added through the schema
	 * changes. See <code>Generator</code> for an explanation of this
	 * representation.
	 */
	private List<List<List<String>>> _logicalSchemas = new ArrayList<List<List<String>>>();

	/**
	 * Tracks the mappings that were added as a result of an addPeer operation.
	 */
	private Map<String, List<String>> _peersToMaps = new HashMap<String, List<String>>();

	/**
	 * Store those mappings that were added with a peer. See
	 * <code>Generator</code> for an explanation of this representation.
	 */
	private List<List<Object>> _mappings = new ArrayList<List<Object>>();

	/**
	 * Mapping from operation number to a <code>List</code> of operations that
	 * were performed in that iteration.
	 */
	private Map<Integer, List<List<String>>> _operations = new HashMap<Integer, List<List<String>>>();

	/**
	 * Record an addPeer operation.
	 * 
	 * @param iteration
	 *            the iteration number.
	 * @param peer
	 *            the peer that was added.
	 * @param logicalSchema
	 *            the logical schema that was added.
	 */
	public void addPeer(int iteration, int peer,
			List<List<String>> logicalSchema) {
		if (null == _operations.get(iteration)) {
			_operations.put(iteration, new ArrayList<List<String>>());
		}
		String peerName = "P" + peer;
		_peers.add(peer);
		_logicalSchemas.add(peer, logicalSchema);
		_operations.get(iteration).add(Arrays.asList("addPeer", peerName));
	}

	/**
	 * Record the mappings that were added with a peer.
	 * 
	 * @param peer
	 *            the peer.
	 * @param mappingNumber
	 *            the mapping number.
	 * @param mapping
	 *            the mapping.
	 */
	public void addMappingForPeer(int peer, int mappingNumber,
			List<Object> mapping) {
		String mappingName = "M" + mappingNumber;
		_mappings.add(Arrays.asList(mappingName, mapping.get(0),
				mapping.get(1), mapping.get(2)));
		String peerName = "P" + peer;
		if (null == _peersToMaps.get("P" + peer)) {
			_peersToMaps.put(peerName, new ArrayList<String>());
		}
		_peersToMaps.get(peerName).add(mappingName);
	}

	/**
	 * Record a deletePeer operation.
	 * 
	 * @param iteration
	 *            the iteration number.
	 * @param peer
	 *            the peer that was deleted.
	 */
	public void deletePeer(int iteration, int peer) {
		if (null == _operations.get(iteration)) {
			_operations.put(iteration, new ArrayList<List<String>>());
		}
		_operations.get(iteration).add(Arrays.asList("deletePeer", "P" + peer));
	}

	/**
	 * Record an addBypass operation.
	 * 
	 * @param iteration
	 *            the iteration number.
	 * @param mappingNumber
	 *            the mapping number.
	 * @param mapping
	 *            the mapping that was added.
	 */
	public void addBypass(int iteration, int mappingNumber, List<Object> mapping) {
		if (null == _operations.get(iteration)) {
			_operations.put(iteration, new ArrayList<List<String>>());
		}
		String mappingName = "M" + mappingNumber;
		_mappings.add(new ArrayList<Object>(Arrays.asList(mappingName, mapping
				.get(0), mapping.get(1), mapping.get(2))));
		_operations.get(iteration).add(Arrays.asList("addBypass", mappingName));
	}

	/**
	 * Record a deleteBypass operation.
	 * 
	 * @param iteration
	 *            the iteration number.
	 * @param mappingNumber
	 *            the number of the mapping that was added.
	 */
	public void deleteBypass(int iteration, int mappingNumber) {
		if (null == _operations.get(iteration)) {
			_operations.put(iteration, new ArrayList<List<String>>());
		}
		_operations.get(iteration).add(
				Arrays.asList("deleteBypass", "M" + mappingNumber));
	}

	/**
	 * Serializes the journal in the format below. The peer and maping elements
	 * are as in the Orchestra schema file.
	 * 
	 * <pre>
	 * 	&lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;?&gt;
	 * 	&lt;!--00:50:07 06/12/08 EDT
	 * 	{integers=1, inout=true, oracle=0, skip=0, cutoff=1024, password=password, addBypasses=1, deletions=1, 
	 *     username=xyz, relsize=15, tukwila=0, fanout=2, olivier=1, schemas=3, dbalias=BIOTBS, insertions=2, 
	 *     iterations=2, seed=0, mappingsServer=jdbc:db2://localhost:50000, bidir=0, updateAlias=null, addPeers=1, 
	 *     coverage=1.0, deleteBypasses=1, maxcycles=-1, peers=3, filename=prot, mincycles=-1, deletePeers=1}--&gt;
	 * 	&lt;deltas&gt;
	 * 	  &lt;iteration idx=&quot;0&quot;&gt;
	 * 	    &lt;operation type=&quot;addPeer&quot; name=&quot;P0&quot;/&gt;
	 * 	    &lt;operation type=&quot;addPeer&quot; name=&quot;P1&quot;&gt;
	 * 	      &lt;mapping name=&quot;M0&quot;/&gt;
	 * 	      &lt;mapping name=&quot;M2&quot;/&gt;
	 * 	    &lt;/operation&gt;
	 * 	    &lt;operation type=&quot;addPeer&quot; name=&quot;P2&quot;&gt;
	 * 	      &lt;mapping name=&quot;M1&quot;/&gt;
	 * 	    &lt;/operation&gt;
	 * 	  &lt;/iteration&gt;
	 * 	  &lt;iteration idx=&quot;1&quot;&gt;
	 * 	    &lt;operation type=&quot;addPeer&quot; name=&quot;P3&quot;&gt;
	 * 	      &lt;mapping name=&quot;M3&quot;/&gt;
	 * 	    &lt;/operation&gt;
	 * 	    &lt;operation type=&quot;deletePeer&quot; name=&quot;P0&quot;/&gt;
	 * 	  &lt;/iteration&gt;
	 * 	  &lt;iteration idx=&quot;2&quot;&gt;
	 * 	    &lt;operation type=&quot;addPeer&quot; name=&quot;P4&quot;&gt;
	 * 	      &lt;mapping name=&quot;M4&quot;/&gt;
	 * 	      &lt;mapping name=&quot;M5&quot;/&gt;
	 * 	      &lt;mapping name=&quot;M6&quot;/&gt;
	 * 	    &lt;/operation&gt;
	 * 	    &lt;operation type=&quot;deletePeer&quot; name=&quot;P2&quot;/&gt;
	 * 	    &lt;operation type=&quot;addBypass&quot; name=&quot;M23&quot;/&gt;
	 * 	    &lt;operation type=&quot;deleteBypass&quot; name=&quot;M2&quot;/&gt;
	 * 	  &lt;/iteration&gt;
	 * 	  &lt;peer name=&quot;P0&quot; address=&quot;localhost&quot;&gt;
	 * 	  &lt;/peer&gt;
	 * 	  &lt;mapping name=&quot;M0&quot; materialized=&quot;true&quot;&gt;
	 * 	  &lt;/mapping&gt;
	 * 	&lt;/deltas&gt;
	 * 
	 * </pre>
	 * 
	 * @param params
	 *            run parameters, see <code>Generator</code>.
	 * @return serialzed <code>GeneratorJournal</code>.
	 */
	@SuppressWarnings("unchecked")
	public Document serialize(Map<String, Object> params) {
		// TODO: this method copies code from MetadataXml, needs refactoring.
		Document deltasDoc = DocumentHelper.createDocument();
		deltasDoc.addComment(WorkloadGeneratorUtils.stamp() + "\n" + params);

		Element deltas = deltasDoc.addElement("deltas");

		List<Integer> indexes = new LinkedList<Integer>(_operations.keySet());
		Collections.sort(indexes);
		for (Integer idx : indexes) {
			Element iteration = deltas.addElement("iteration").addAttribute(
					"idx", idx.toString());
			List<List<String>> operations = _operations.get(idx);
			for (List<String> operation : operations) {
				Element opElement = iteration.addElement("operation")
						.addAttribute("type", operation.get(0)).addAttribute(
								"name", operation.get(1));
				if ("addPeer".equals(operation.get(0))) {
					if (null == _peersToMaps.get(operation.get(1))) {
						continue;
					}
					for (String mapping : _peersToMaps.get(operation.get(1))) {
						opElement.addElement("mapping").addAttribute("name",
								mapping);
					}
				}
			}
		}

		for (int i = 0; i < _peers.size(); i++) {
			Element peer = deltas.addElement("peer").addAttribute("name",
					"P" + i).addAttribute("address",
					"localhost");
			int j = _peers.get(i);
			Element schema = peer.addElement("schema").addAttribute("name",
					"S" + j);
			for (int k = 0; k < _logicalSchemas.get(j).size(); k++) {
				for (String var : iovariations((Boolean) params.get("inout"))) {
					Element relation = schema.addElement("relation")
							.addAttribute(
									"name",
									WorkloadGeneratorUtils.relname(i, j, k)
											+ var).addAttribute("materialized",
									"true");
					
					if((Double) params.get("coverage") == 1){
						relation.addAttribute("noNulls", "true");
					}
					
					String hasLocalData;
					if(Generator.peerHasLocalData(i, (Integer) params.get("topology"), 
							(Integer) params.get("modlocal"), (Integer) params.get("peers"), (Integer) params.get("fanout"))) {
						hasLocalData = "true";
					}else{
						hasLocalData = "false";
					}
					relation.addAttribute("hasLocalData",
							hasLocalData);
					relation.addElement("dbinfo").addAttribute("schema",
							(String) params.get("username")).addAttribute(
							"table",
							WorkloadGeneratorUtils.relname(i, j, k) + var);
					relation.addElement("field").addAttribute("name", "KID")
							.addAttribute("type", "integer").addAttribute(
									"key", "true");
					for (String att : _logicalSchemas.get(j).get(k)) {
						relation.addElement("field").addAttribute("name", att)
								.addAttribute("type",
										MetadataXml.universalType(att, params));
					}
				}
			}
		}

		for (int k = 0; k < _mappings.size(); k++) {
			int i = (Integer) _mappings.get(k).get(1);
			int j = (Integer) _mappings.get(k).get(2);

			List<String> x = (List<String>) _mappings.get(k).get(3);
			List<String> source = MetadataXml.selectAtoms(i, "KID", x, "_",
					_logicalSchemas, _peers, (Boolean)params.get("addValueAttr"), true);
			// _ means don't care
			List<String> target = MetadataXml.selectAtoms(j, "KID", x, "-",
					_logicalSchemas, _peers, (Boolean)params.get("addValueAttr"), false);
			// - means null
			Element mapping = deltas.addElement("mapping").addAttribute("name",
					"M" + k).addAttribute("materialized", "true");
			if (1 == (Integer) params.get("bidir")) {
				mapping.addAttribute("bidirectional", "true");
			}
			Element head = mapping.addElement("head");
			for (String atom : target) {
				Element atomElem = head.addElement("atom");
				if (1 == (Integer) params.get("bidir")) {
					atomElem.addAttribute("del", "true");
				}
				atomElem.addText(atom);
			}
			Element body = mapping.addElement("body");
			for (String atom : source) {
				Element atomElem = body.addElement("atom");
				if (1 == (Integer) params.get("bidir")) {
					atomElem.addAttribute("del", "true");
				}
				atomElem.addText(atom);
			}
		}

		return deltasDoc;

	}

	/**
	 * Write this <code>GeneratorJournal</code> out to <code>writer</code>.
	 * See <code>serialize(...)</code> for the format.
	 * 
	 * @param writer
	 *            to which to write this <code>GeneratorJournal</code>.
	 * @param params
	 *            run parameters, see <code>Generator</code>.
	 * @throws IOException
	 *             if such is thrown whilst writing this
	 *             <code>GeneratorJournal</code>.
	 */
	public void write(Writer writer, Map<String, Object> params)
			throws IOException {
		XMLWriter xmlWriter = new XMLWriter(writer, OutputFormat
				.createPrettyPrint());
		xmlWriter.write(serialize(params));
		xmlWriter.flush();
		xmlWriter.close();
	}

	private List<String> iovariations(boolean inout) {
		if (inout) {
			return new ArrayList<String>(Arrays.asList(new String[] { "", "_L",
					"_R" }));
		} else {
			return new ArrayList<String>(Arrays.asList(new String[] { "" }));
		}
	}

	/**
	 * Return the peers that have been recorded in this
	 * <code>GeneratorJournal</code>.
	 * 
	 * @return see method description.
	 */
	public List<Integer> getPeers() {
		return _peers;
	}

	/**
	 * Return the logical schemas that have been recorded in this
	 * <code>GeneratorJournal</code>.
	 * 
	 * @return see method description.
	 */
	public List<List<List<String>>> getLogicalSchemas() {
		return _logicalSchemas;
	}
}
