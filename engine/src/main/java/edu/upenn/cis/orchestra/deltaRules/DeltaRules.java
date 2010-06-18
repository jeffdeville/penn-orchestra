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
package edu.upenn.cis.orchestra.deltaRules;

import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeDatalogSequenceList;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.SingleRuleDatalogProgram;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Rules for incremental update exchange.
 * 
 * @author zives, gkarvoun, John Frommeyer
 * 
 */
public abstract class DeltaRules implements IDeltaRules {

	private final List<DatalogSequence> code;

	/**
	 * Create executable delta rules from {@code code}.
	 * 
	 * @param code
	 */
	protected DeltaRules(@SuppressWarnings("hiding") List<DatalogSequence> code) {
		this.code = Collections.unmodifiableList(code);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.deltaRules.IDeltaRules#cleanupPreparedStmts()
	 */
	@Override
	public void cleanupPreparedStmts() {
		List<DatalogSequence> prog = getCode();

		if (prog != null) {
			for (DatalogSequence ds : prog) {
				for (Datalog d : ds.getSequence()) {
					if (d instanceof DatalogProgram) {
						DatalogProgram dp = (DatalogProgram) d;
						RuleQuery rq = dp.statements();
						if (rq != null)
							rq.cleanupPrepared();
					} else if (d instanceof SingleRuleDatalogProgram) {
						SingleRuleDatalogProgram dp = (SingleRuleDatalogProgram) d;
						RuleQuery rq = dp.statements();
						if (rq != null)
							rq.cleanupPrepared();
					}
				}
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.deltaRules.IDeltaRules#execute(edu.upenn.cis.
	 * orchestra.datalog.DatalogEngine)
	 */
	@Override
	abstract public long execute(DatalogEngine de) throws Exception;

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.deltaRules.IDeltaRules#getCode()
	 */
	@Override
	public List<DatalogSequence> getCode() {
		return code;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.deltaRules.IDeltaRules#serialize()
	 */
	@Override
	public Document serialize() {
		Document doc = null;
		try {
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder;
			builder = builderFactory.newDocumentBuilder();
			doc = builder.newDocument();
			Element ruleGen = doc.createElement("deltaRules");
			doc.appendChild(ruleGen);
			for (DatalogSequence ds : code) {
				ruleGen.appendChild(ds.serialize(doc));
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		return doc;
	}

	/**
	 * 
	 * Returns the {@code IDeltaRules} represented by {@code doc}.
	 * 
	 * @param doc
	 * @param system 
	 * @return Returns the {@code IDeltaRules} represented by {@code doc}
	 * @throws XMLParseException
	 */
	public static IDeltaRules deserialize(Document doc, OrchestraSystem system)
			throws XMLParseException {
		Element root = doc.getDocumentElement();
		if (!"deltaRules".equals(root.getNodeName())) {
			throw new XMLParseException(
					"Root element should be 'deltaRules' not '"
							+ root.getNodeName() + "'.");
		}
		String rulesType = root.getAttribute("type");
		List<Element> datalogElements = getChildElementsByName(root, "datalog");
		List<DatalogSequence> code = deserializeDatalogSequenceList(datalogElements, system);
		IDeltaRules rules = null;
		if ("Insertion".equals(rulesType)) {
			rules = new InsertionDeltaRules(code);
		} else if ("Deletion".equals(rulesType)) {
			rules = new DeletionDeltaRules(code, system.isBidirectional());
		} else {
			throw new XMLParseException("Unrecognized IDeltaRules type: ["
					+ rulesType + "].");
		}
		return rules;
	}

	@Override
	public Document serializeAsCode() {
		Document doc = createDocument();
		Element statements = doc.createElement("statements");
		doc.appendChild(statements);
		for (DatalogSequence sequence : code) {
			List<Element> seqElements = sequence.serializeAsCode(doc);
			for (Element seqElement : seqElements) {
				statements.appendChild(seqElement);
			}
		}
		return doc;
	}

}
