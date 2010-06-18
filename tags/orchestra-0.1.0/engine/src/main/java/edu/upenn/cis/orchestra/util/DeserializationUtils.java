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
package edu.upenn.cis.orchestra.util;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.util.List;

import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * A central place for deserializing lists of objects.
 * 
 * @author John Frommeyer
 * 
 */
public class DeserializationUtils {

	private interface IDeserializer<T> {

		public T deserialize(Element child, OrchestraSystem system)
				throws XMLParseException;

		public String getTagname();

	}

	private DeserializationUtils() {};

	private static final IDeserializer<Mapping> MAPPING = new IDeserializer<Mapping>() {

		@Override
		public Mapping deserialize(Element child, OrchestraSystem system)
				throws XMLParseException {
			return Mapping.deserializeVerboseMapping(child, system);
		}

		@Override
		public String getTagname() {
			return "mapping";
		}
	};

	private static final IDeserializer<Rule> RULE = new IDeserializer<Rule>() {

		@Override
		public Rule deserialize(Element child, OrchestraSystem system)
				throws XMLParseException {
			return Rule.deserializeVerboseRule(child, system);
		}

		@Override
		public String getTagname() {
			return "mapping";
		}
	};

	private static final IDeserializer<RelationContext> RELATION_CONTEXT = new IDeserializer<RelationContext>() {

		@Override
		public RelationContext deserialize(Element child, OrchestraSystem system)
				throws XMLParseException {
			return RelationContext.deserialize(child, system);
		}

		@Override
		public String getTagname() {
			return "relationContext";
		}
	};

	private static final IDeserializer<DatalogSequence> DATALOG_SEQUENCE = new IDeserializer<DatalogSequence>() {

		@Override
		public DatalogSequence deserialize(Element child, OrchestraSystem system)
				throws XMLParseException {
			return DatalogSequence.deserialize(child, system);
		}

		@Override
		public String getTagname() {
			return "datalog";
		}

	};

	private static <T> List<T> deserializeListElement(IDeserializer<T> d,
			Element e, OrchestraSystem system) throws XMLParseException {
		List<Element> children = getChildElementsByName(e, d.getTagname());
		return deserializeList(d, children, system);
	}

	private static <T> List<T> deserializeList(IDeserializer<T> d,
			List<Element> elements, OrchestraSystem system)
			throws XMLParseException {
		List<T> result = newArrayList();
		for (Element child : elements) {
			result.add(d.deserialize(child, system));
		}
		return result;
	}

	/**
	 * Returns the list of {@code Mapping}s represented by the child elements of
	 * {@code mappingsElement}.
	 * 
	 * @param mappingsElement an {@code Element}, every child of which, was
	 *            produced by {@code Mapping.serializeVerbose(Document)}
	 * @param system
	 * @return the list of {@code Mapping}s represented by the child elements of
	 *         {@code mappingsElement}
	 * @throws XMLParseException
	 */
	public static List<Mapping> deserializeVerboseMappings(
			Element mappingsElement, OrchestraSystem system)
			throws XMLParseException {
		return deserializeListElement(MAPPING, mappingsElement, system);
	}

	/**
	 * Returns the list of {@code Rule}s represented by the child elements of
	 * {@code mappingsElement}.
	 * 
	 * @param mappingsElement an {@code Element}, every child of which, was
	 *            produced by {@code Rule.serializeVerbose(Document)}
	 * @param system
	 * @return the list of {@code Rule}s represented by the child elements of
	 *         {@code mappingsElement}
	 * @throws XMLParseException
	 */
	public static List<Rule> deserializeVerboseRules(Element mappingsElement,
			OrchestraSystem system) throws XMLParseException {
		return deserializeListElement(RULE, mappingsElement, system);
	}

	/**
	 * Returns the list of {@code RelationContext}s represented by the child
	 * elements of {@code contextsElement}.
	 * 
	 * @param contextsElement an {@code Element}, every child of which, was
	 *            produced by {@code RelationContext.serialize(Document)}
	 * @param system
	 * @return the list of {@code RelationContext}s represented by the child
	 *         elements of {@code contextsElement}
	 * @throws XMLParseException
	 */
	public static List<RelationContext> deserializeRelationContexts(
			Element contextsElement, OrchestraSystem system)
			throws XMLParseException {
		return deserializeListElement(RELATION_CONTEXT, contextsElement, system);
	}

	/**
	 * Returns a list of {@code DatalogSequence} corresponding to the list of
	 * {@code Element} given in {@code datalogList}.
	 * 
	 * @param datalogList
	 * @param system 
	 * @return a list of {@code DatalogSequence} corresponding to the list of
	 *         {@code Element} given in {@code datalogList}
	 * @throws XMLParseException
	 */
	public static List<DatalogSequence> deserializeDatalogSequenceList(
			List<Element> datalogList, OrchestraSystem system) throws XMLParseException {
		return deserializeList(DATALOG_SEQUENCE, datalogList, system);
	}

}
