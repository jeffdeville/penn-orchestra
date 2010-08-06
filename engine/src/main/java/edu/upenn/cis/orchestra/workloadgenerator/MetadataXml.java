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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import edu.upenn.cis.orchestra.datamodel.Relation;

public class MetadataXml {

	private Map<String, Object> _params;

	private boolean _inout;

	public static int uniqueId = 0;

	public MetadataXml(Map<String, Object> params, boolean inout) {
		_params = params;
		_inout = inout;
	}

	public void metadataXml(List<List<List<String>>> schemas,
			List<Integer> peers, List<List<Object>> mappings) {
		metadataXml(schemas, peers, mappings, "");
	}

	@SuppressWarnings("unchecked")
	public void metadataXml(List<List<List<String>>> schemas,
			List<Integer> peers, List<List<Object>> mappings, String extension) {

		Writer writer = null;
		if (null == _params.get("filename")) {
			writer = new PrintWriter(System.out);
		} else {
			try {
				if (!"".equals(extension)) { 
					extension = "." + extension;
				}
				writer = new FileWriter((String) _params.get("filename") 
						+ extension + ".schema");
				// + ".schema.new");
			} catch (IOException e) {
				throw new IllegalStateException(
						"Unable to create schema file.", e);
			}
		}

		Document document = DocumentHelper.createDocument();
		document.addComment(WorkloadGeneratorUtils.stamp() + "\n" + _params);
		Element catalog = document.addElement("catalog").addAttribute(
				"recMode", "false").addAttribute("name",
						(String) _params.get("filename"));
		catalog.addElement("migrated").setText("true");
		for (int i = 0; i < peers.size(); i++) {
			if (null == peers.get(i)) {
				continue;
			}
			Element peer = catalog.addElement("peer").addAttribute("name",
					"P" + i).addAttribute("address",
					"localhost");
			int j = peers.get(i);
			Element schema = peer.addElement("schema").addAttribute("name",
					"S" + j);
			for (int k = 0; k < schemas.get(j).size(); k++) {
				for (String var : iovariations()) {
					Element relation = schema.addElement("relation")
					.addAttribute(
							"name",
							WorkloadGeneratorUtils.relname(i, j, k)
							+ var).addAttribute("materialized",
							"true");
					if((Double) _params.get("coverage") == 1){
						relation.addAttribute("noNulls", "true");
					}
						
					String hasLocalData;
					if(Generator.peerHasLocalData(i, (Integer) _params.get("topology"), 
							(Integer) _params.get("modlocal"), (Integer) _params.get("peers"), (Integer) _params.get("fanout"))) {
						hasLocalData = "true";
					}else{
						hasLocalData = "false";
					}
			
					relation.addAttribute("hasLocalData",
							hasLocalData);
					relation.addElement("dbinfo").addAttribute("schema",
							(String) _params.get("username")).addAttribute(
									"table",
									WorkloadGeneratorUtils.relname(i, j, k) + var);
					relation.addElement("field").addAttribute("name", "KID")
					.addAttribute("type", "integer").addAttribute(
							"key", "true");
					for (String att : schemas.get(j).get(k)) {
						if((Boolean)_params.get("addValueAttr") && att.equals(Relation.valueAttrName)){
							relation.addElement("field").addAttribute("name", att)
							.addAttribute("type", "integer").addAttribute(
									"key", "true");
						}else{
							relation.addElement("field").addAttribute("name", att)
							.addAttribute("type",
									universalType(att, _params));
						}
					}
				}
			}
		}

		for (int k = 0; k < mappings.size(); k++) {
			if (null == mappings.get(k)) { 
				continue;
			}
			int i = (Integer) mappings.get(k).get(0);
			int j = (Integer) mappings.get(k).get(1);

			List<String> x = (List<String>) mappings.get(k).get(2);
			List<String> source = selectAtoms(i, "KID", x, "_", schemas, peers, (Boolean)_params.get("addValueAttr"), true);
			// _ means don't care
			List<String> target = selectAtoms(j, "KID", x, "-", schemas, peers, (Boolean)_params.get("addValueAttr"), false);
			// - means null
			Element mapping = catalog.addElement("mapping").addAttribute(
					"name", "M" + k).addAttribute("materialized", "true");
			if (1 == (Integer) _params.get("bidir")) {
				mapping.addAttribute("bidirectional", "true");
			}
			Element head = mapping.addElement("head");
			for (String atom : target) {
				Element atomElem = head.addElement("atom");
				if (1 == (Integer) _params.get("bidir")) {
					atomElem.addAttribute("del", "true");
				}
				atomElem.addText(atom);
			}
			Element body = mapping.addElement("body");
			for (String atom : source) {
				Element atomElem = body.addElement("atom");
				if (1 == (Integer) _params.get("bidir")) {
					atomElem.addAttribute("del", "true");
				}
				atomElem.addText(atom);
			}
		}

		Element mappingsElem = catalog.addElement("engine").addElement(
		"mappings");
		if (1 == (Integer) _params.get("tukwila")) {
			mappingsElem.addAttribute("type", "tukwila").addAttribute("host",
			"localhost").addAttribute("port", "7777");
		} else {
			mappingsElem.addAttribute("type", "sql").addAttribute("server",
					(String) _params.get("mappingsServer") + "/"
					// "jdbc:db2://localhost:50000/"
					+ _params.get("dbalias")).addAttribute("username",
							(String) _params.get("username")).addAttribute("password",
									(String) _params.get("password"));
		}
		if (null != _params.get("updateAlias")) {
			catalog.addElement("updates").addAttribute(
					"server",
					"jdbc:db2://localhost:50000/"
					+ _params.get("updateAlias")).addAttribute(
							"username", (String) _params.get("username")).addAttribute(
									"password", (String) _params.get("password"));
		}

		// Output some default (dummy) reconciliation store info
		Element store = catalog.addElement("store");
		store.addElement("update").addAttribute("type", "bdb").addAttribute(
				"hostname", "localhost").addAttribute("port", "777");
		store.addElement("state").addAttribute("type", "hash");

		// Output trust conditions saying that everyone trusts everyone
		for (int i = 0; i < peers.size(); i++) {
			if (null == peers.get(i)) {
				continue;
			}
			int j = peers.get(i);
			Element trustConditions = catalog.addElement("trustConditions")
			.addAttribute("peer", "P" + i).addAttribute("schema",
					"S" + j);
			for (int i2 = 0; i2 < peers.size(); i2++) {
				if (null == peers.get(i2)) {
					continue;
				}
				int j2 = peers.get(i2);
				if (i != i2) {
					for (int k2 = 0; k2 < schemas.get(j2).size(); k2++) {
						trustConditions.addElement("trusts").addAttribute(
								"pid", "P" + i2).addAttribute("pidType",
								"STRING").addAttribute("pidType", "STRING")
								.addAttribute("priority", "5").addAttribute(
										"relation",
										WorkloadGeneratorUtils.relname(i2, j2,
												k2));
					}
				}
			}
		}

		try {
			OutputFormat format = OutputFormat.createPrettyPrint();
			XMLWriter xmlWriter = new XMLWriter(writer, format);
			xmlWriter.write(document);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw new IllegalStateException("Problem writing schema file.", e);
		}
	}

	private List<String> iovariations() {
		if (_inout) {
			if(1 == (Integer) _params.get("noreject"))
				if(1 == (Integer) _params.get("nolocal"))
					return new ArrayList<String>(Arrays.asList(new String[] { "" }));
				else
					return new ArrayList<String>(Arrays.asList(new String[] { "", "_L" }));
			else if(1 == (Integer) _params.get("nolocal"))
				return new ArrayList<String>(Arrays.asList(new String[] { "", "_R" }));
			else
				return new ArrayList<String>(Arrays.asList(new String[] { "", "_L", "_R" }));
		} else {
			return new ArrayList<String>(Arrays.asList(new String[] { "" }));
		}
	}

	private static String printrel(int i, int j, int k, List<String> a) {
		return WorkloadGeneratorUtils.ppart(i) + printsrel(j, k, a);
	}

	private static String printsrel(int i, int j, List<String> a) {
		StringBuilder sb = new StringBuilder(WorkloadGeneratorUtils.spart(i)
				+ WorkloadGeneratorUtils.rpart(j) + "(");
		for (Iterator<String> itr = a.iterator(); itr.hasNext();) {
			sb.append(itr.next());
			if (itr.hasNext()) {
				sb.append(", ");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	static String universalType(String a, Map<String, Object> params) {
		if (1 == (Integer) params.get("integers") || ((Boolean)params.get("addValueAttr") && a.equals(Relation.valueAttrName))) {
			return "integer";
		} else {
			int length = Math.min(Stats.getSize(a), (Integer) params
					.get("cutoff"));
			return "varchar(" + length + ")";
		}
	}

	static List<String> selectAtoms(int i, String key, List<String> x,
			String ch, List<List<List<String>>> schemas, List<Integer> peers,
			boolean addValueAttr, boolean source) {
		Set<String> xSet = new HashSet<String>(x);
		List<String> atoms = new ArrayList<String>();

		if (null == peers.get(i)) {
			return atoms;
		}
		for (int k = 0; k < schemas.get(peers.get(i)).size(); k++) {
			List<String> rel = schemas.get(peers.get(i)).get(k);
			Set<String> relSet = new HashSet<String>(rel);
			relSet.retainAll(xSet);
			if (0 != relSet.size()) {
				List<String> atts = new ArrayList<String>(Arrays
						.asList(new String[] { key }));
				for (String att : rel) {
					if (xSet.contains(att)) {
						if(addValueAttr && Relation.valueAttrName.equals(att) ){
							if(source){
								atts.add(att + uniqueId);
								uniqueId++;
							}else{
								atts.add("0");
							}
						}else{
							atts.add(att);
						}
					} else {
						atts.add(ch);
					}
				}
				atoms.add("P" + i + ".S" + peers.get(i) + "."
						+ printrel(i, peers.get(i), k, atts));
			}
		}
		return atoms;
	}

	public static List<String> iovariations(boolean inout) {

		if (inout) {
			return new ArrayList<String>(Arrays.asList(new String[] { "", "_L",
			"_R" }));
		} else {
			return new ArrayList<String>(Arrays.asList(new String[] { "" }));
		}
	}

}
