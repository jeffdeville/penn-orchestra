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
package edu.upenn.cis.orchestra.datalog;

import java.util.Collection;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * 
 * @author gkarvoun
 *
 */
public abstract class Datalog {
	protected boolean _c4f = false;
	protected boolean _measureExecTime = false;
	
	public abstract void setMeasureExecTime(boolean s);
	public abstract boolean measureExecTime();
	
	public abstract boolean count4fixpoint();

	/**
	 * Returns an {@code Element} which represents this {@code Datalog}.
	 * 
	 * @param document only used to create {@code Element}s
	 * @return an {@code Element} which represents this {@code Datalog}
	 */
	public Element serialize(Document document) {
		Element e = document.createElement("datalog");
		e.setAttribute("countForFixpoint", Boolean.toString(_c4f));
		e.setAttribute("measureExecTime", Boolean.toString(_measureExecTime));
		return e;
	}

	/**
	 * Returns the {@code Datalog} represented by {@code datalog}.
	 * 
	 * @param datalog an {@code Element} produced by {@code serialize(Document)}
	 * @param system 
	 * @return the {@code Datalog} represented by {@code datalog}
	 * @throws XMLParseException 
	 */
	public static Datalog deserialize(Element datalog, OrchestraSystem system) throws XMLParseException {
		Datalog result = null;
		String type = datalog.getAttribute("type");
		if ("datalogProgram".equals(type)) {
			result = DatalogProgram.deserialize(datalog, system);
		} else if ("datalogSequence".equals(type)) {
			result = DatalogSequence.deserialize(datalog, system);
		} else if ("".equals(type)) {
			throw new XMLParseException("Missing 'type' attribute of 'datalog' element.");
		}else {
			throw new XMLParseException("Unknown Datalog type: " + type + ".");
		}
		return result;
	}

	/**
	 * Returns a XML representation of the code this datalog transforms
	 * into.
	 * 
	 * @param doc only used for {@code Element} creation
	 * @return a XML representation of the code this datalog transforms
	 *         into
	 */
	public abstract List<Element> serializeAsCode(Document doc);
}
