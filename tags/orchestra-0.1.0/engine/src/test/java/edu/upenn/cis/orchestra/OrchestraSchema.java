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
package edu.upenn.cis.orchestra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.util.DomUtils;

/**
 * A simple representation of an Orchestra schema file to be used for testing.
 * 
 * @author John Frommeyer
 * 
 */
public class OrchestraSchema {

	/** The represented Orchestra schema file. */
	private final Document orchestraSchema;

	/** The name of this Orchestra schema. */
	private String orchestraSchemaName;

	/** The set of database schema names from {@code orchestraSchema}. */
	private final Set<String> dbSchemaNames = OrchestraUtil.newHashSet();

	/**
	 * The set of database schema name regular expressions from {@code
	 * orchestraSchema}.
	 */
	private final Set<String> dbSchemaNameRegexps = OrchestraUtil
			.newHashSet();

	/**
	 * Maps qualified table name to the corresponding relation element from
	 * {@code orchestraSchema}.
	 */
	private final Map<String, Element> tableNameToElement = OrchestraUtil
			.newHashMap();

	/**
	 * Creates an {@code OrchestraSchema} using the contents of {@code
	 * orchestraSchemaFile}.
	 * 
	 * @param orchestraSchemaFile
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 * @throws XPathExpressionException
	 */
	public OrchestraSchema(File orchestraSchemaFile) throws SAXException,
			IOException, ParserConfigurationException, XPathExpressionException {

		orchestraSchema = fileToDocument(orchestraSchemaFile);
		populateCollections();
	}

	/**
	 * Returns the set of schema names in this Orchestra schema. The names are
	 * the actual database names as found in the {@code dbinfo} element. If
	 * {@code regexp == true} then each name is appended with ".*".
	 * 
	 * @param regexp indicates that regular expressions are desired.
	 * 
	 * @return the set of schema names in this Orchestra schema.
	 */
	public Set<String> getDbSchemaNames(boolean regexp) {
		return Collections.unmodifiableSet(regexp ? dbSchemaNameRegexps
				: dbSchemaNames);
	}

	/**
	 * Returns the list of qualified table names in this Orchestra schema. The
	 * names are the actual database names, that is, they are in {@code
	 * <dbinfo/@schema>.<dbinfo/@table>} form.
	 * 
	 * @return the list of qualified relation names in this Orchestra schema.
	 */
	public Set<String> getDbTableNames() {
		return Collections.unmodifiableSet(tableNameToElement.keySet());
	}

	/**
	 * Returns the enclosing {@code relation} element of the database table with
	 * name {@code qualifiedTableName}.
	 * 
	 * @param qualifiedTableName table name in {@code
	 *            <dbinfo/@schema>.<dbinfo/@table>} form.
	 * @return the enclosing {@code relation} element of the database table with
	 *         name {@code qualifiedTableName}.
	 */
	public Element getElementForName(String qualifiedTableName) {
		return tableNameToElement.get(qualifiedTableName);
	}

	/**
	 * Returns the underlying Orchestra schema as a {@code Document}.
	 * 
	 * @return the underlying Orchestra schema as a {@code Document}.
	 */
	public Document toDocument() {
		return (Document) orchestraSchema.cloneNode(true);
	}

	/**
	 * Returns the underlying Orchestra schema as a {@code Document} with any
	 * existing {@code /catalog/engine/mappings} elements are replaced by one
	 * which has been created on the fly using the passed in parameters.
	 * 
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @return the underlying Orchestra schema as a {@code Document}
	 */
	public Document toDocument(String dbURL, String dbUser, String dbPassword) {
		return TestUtil.replaceMappingElement(orchestraSchema, dbURL, dbUser,
				dbPassword, "sql");

	}

	/**
	 * Returns the underlying Orchestra schema as a {@code Document} with any
	 * existing {@code /catalog/engine/mappings} elements are replaced by one
	 * which has been created on the fly using the passed in parameters. The
	 * peer {@code localPeer} is designated as the local peer.
	 * 
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @param localPeer 
	 * @return the underlying Orchestra schema as a {@code Document}
	 */
	public Document toDocument(String dbURL, String dbUser, String dbPassword,
			String localPeer) {
		return TestUtil.setLocalPeer(TestUtil.replaceMappingElement(
				orchestraSchema, dbURL, dbUser, dbPassword, "sql"), localPeer);

	}

	/**
	 * Writes the underlying Orchestra schema to {@code file}.
	 * 
	 * @param file
	 * @throws IOException
	 */
	public void write(File file) throws IOException {
		DomUtils.write(orchestraSchema, new FileWriter(file));
	}

	/**
	 * Writes the underlying Orchestra schema to {@code file}. Any existing
	 * {@code /catalog/engine/mappings} elements are replaced by one which has
	 * been created on the fly using the passed in parameters.
	 * 
	 * @param file
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws IOException
	 */
	public void write(File file, String dbURL, String dbUser, String dbPassword)
			throws IOException {
		DomUtils.write(TestUtil.replaceMappingElement(orchestraSchema, dbURL,
				dbUser, dbPassword, "sql"), new FileWriter(file));
	}
	
	/**
	 * Writes the underlying Orchestra schema to {@code file}. Any existing
	 * {@code /catalog/engine/mappings} elements are replaced by one which has
	 * been created on the fly using the passed in parameters.
	 * 
	 * @param file
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @param localPeer 
	 * @throws IOException
	 */
	public void write(File file, String dbURL, String dbUser, String dbPassword, String localPeer)
			throws IOException {
		DomUtils.write(TestUtil.setLocalPeer(TestUtil.replaceMappingElement(orchestraSchema, dbURL,
				dbUser, dbPassword, "sql"), localPeer), new FileWriter(file));
	}

	/**
	 * Returns the underlying Orchestra schema as a XML {@code String}.
	 * 
	 * @return the underlying Orchestra schema as a XML {@code String}.
	 * 
	 */
	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		DomUtils.write(orchestraSchema, sw);
		return sw.toString();
	}

	/**
	 * Returns the underlying Orchestra schema as a XML {@code String}. Any
	 * existing {@code /catalog/engine/mappings} elements are replaced by one
	 * which has been created on the fly using the passed in parameters.
	 * 
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * 
	 * @return the underlying Orchestra schema as a XML {@code String}.
	 * 
	 */
	public String toString(String dbURL, String dbUser, String dbPassword) {
		StringWriter sw = new StringWriter();
		DomUtils.write(TestUtil.replaceMappingElement(orchestraSchema, dbURL,
				dbUser, dbPassword, "sql"), sw);
		return sw.toString();
	}

	/**
	 * Helper for constructors.
	 * 
	 * @throws XPathExpressionException
	 */
	private void populateCollections() throws XPathExpressionException {

		// orchestraSchemaName =
		// orchestraSchema.getFirstChild().getAttributes().getNamedItem("name").getNodeValue();
		// Get database schema names.
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		NodeList relations = (NodeList) xpath.evaluate("//relation",
				orchestraSchema, XPathConstants.NODESET);

		for (int i = 0; i < relations.getLength(); i++) {
			Element relation = (Element) relations.item(i);
			Element dbinfo = DomUtils.getChildElementByName(relation, "dbinfo");
			String schema = dbinfo.getAttribute("schema");
			String table = dbinfo.getAttribute("table");
			dbSchemaNames.add(schema);
			dbSchemaNameRegexps.add(schema + ".*");
			tableNameToElement.put(schema + "." + table, relation);
		}
		XPath namePath = xpathFactory.newXPath();
		orchestraSchemaName = namePath.evaluate("/catalog/@name",
				orchestraSchema);

	}

	/**
	 * Returns {@code} file as a {@code Document}.
	 * 
	 * @param file
	 * @return {@code} file as a {@code Document}.
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	private static Document fileToDocument(File file)
			throws ParserConfigurationException, SAXException, IOException {
		FileInputStream in = new FileInputStream(file);
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder builder = builderFactory.newDocumentBuilder();
		Document doc = builder.parse(in);

		return doc;
	}

	/**
	 * Returns the name of this Orchestra schema.
	 * 
	 * @return the name of this Orchestra schema.
	 */
	public String getName() {
		return orchestraSchemaName;
	}
}
