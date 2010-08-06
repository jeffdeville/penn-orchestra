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

import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static edu.upenn.cis.orchestra.util.DomUtils.write;

import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * A request object for {@code BerkeleyDBStoreServer}. Used to send the server a
 * Peer/Schema/Relation document as in a {@code .schema} file.
 * <p>
 * Root element name should be the name of the CDSS being defined. The child
 * elements are {@code peer} elements as in {@code .schema} files.
 * 
 * @author John Frommeyer
 * 
 */
class LoadSchemas implements Serializable {

	private static final long serialVersionUID = 1L;
	private final String peerDocumentString;

	LoadSchemas(Document peerDocument) {
		StringWriter sw = new StringWriter();
		write(peerDocument, sw);
		peerDocumentString = sw.toString();

	}

	Document getPeerDocument() {
		Document peerDocument = createDocument(new InputSource(
				new StringReader(peerDocumentString)));
		return peerDocument;
	}

}
