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

import java.io.StringReader;

import javax.xml.transform.Result;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import org.custommonkey.xmlunit.Diff;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.util.DomUtils;

/**
 * An implementation of XMLUnit's {@code Diff} which ignores the whitespace-only
 * text nodes when comparing {@code Document}s.
 * 
 * @author John Frommeyer
 * 
 */
public class IgnoreWhitespaceTextNodesDiff extends Diff {

	// This should strip all whitespace-only text nodes and copy everything
	// else.
	private final static String stylesheet = "<xsl:stylesheet version='1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>"
			+ "<xsl:output method='xml' omit-xml-declaration='yes'/>"
			+ "<xsl:strip-space elements='*'/>"
			+ "<xsl:template match='@*|node()'>"
			+ "<xsl:copy>"
			+ "<xsl:apply-templates select='@*|node()'/>"
			+ "</xsl:copy>"
			+ "</xsl:template>" + "</xsl:stylesheet>";

	private final static Templates template;
	static {
		TransformerFactory tfact = TransformerFactory.newInstance();
		try {
			template = tfact.newTemplates(new StreamSource(new StringReader(
					stylesheet)));
		} catch (TransformerConfigurationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * Creates a {@code Diff} which will strip out any whitespace text nodes
	 * from the input {@code Document}s before performing its comparison.
	 * 
	 * @param controlDoc
	 * @param testDoc
	 */
	public IgnoreWhitespaceTextNodesDiff(Document controlDoc, Document testDoc) {
		super(stripWhitespace(controlDoc), stripWhitespace(testDoc));
		//overrideElementQualifier(new RecursiveElementNameAndTextQualifier());
	}

	private static Document stripWhitespace(Document original) {
		Document stripped = DomUtils.createDocument();
		Result result = new DOMResult(stripped);
		try {
			Transformer trans = template.newTransformer();
			trans.transform(new DOMSource(original), result);
			// trans.transform(new DOMSource(original), new StreamResult(
			// System.out));
			return stripped;
		} catch (TransformerConfigurationException e) {
			throw new IllegalStateException(e);
		} catch (TransformerException e) {
			throw new IllegalStateException(e);
		}
	}
}
