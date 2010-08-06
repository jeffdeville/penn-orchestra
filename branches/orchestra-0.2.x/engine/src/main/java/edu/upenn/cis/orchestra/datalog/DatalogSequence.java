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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Represents a sequence of Datalog programs (or sub-sequences)
 * 
 * @author zives, gkarvoun
 *
 */
public class DatalogSequence extends Datalog {
	List<Datalog> _programs;
	boolean _isRecursive;
//	public boolean _c4f;
//	public boolean _measureExecTime;
	
	/**
	 * Basic datalog sequence
	 * 
	 * @param isRecursive
	 * @param c4f Count for fixpoint in bookkeeping
	 */
	public DatalogSequence(boolean isRecursive, boolean c4f) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		_c4f = c4f;
		_measureExecTime = false;
	}
	
	public DatalogSequence(boolean isRecursive, List<Datalog> p, boolean c4f) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		addAll(p);
		_c4f = c4f;
		_measureExecTime = false;
	}
	
	public DatalogSequence(boolean isRecursive, List<Datalog> p, boolean c4f, boolean t) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		addAll(p);
		_c4f = c4f;
		_measureExecTime = t;
	}

	public void setMeasureExecTime(boolean s){
		_measureExecTime = s;
	}
	
	public boolean measureExecTime(){
		return _measureExecTime;
	}
	
	public boolean count4fixpoint(){
		return _c4f;
	}
	
	public boolean isRecursive() {
		return _isRecursive;
	}
	
	public void setRecursive() {
		_isRecursive = true;
	}
	
	public void clearRecursive() {
		_isRecursive = false;
	}
	
	public void add(DatalogProgram p) {
		_programs.add(p);
	}

	/**
	 * Adds a sub-sequence.  If it's not recursive, we
	 * simply fold it in.  If it is recursive, we keep it nested.
	 * 
	 * @param p sub-sequence
	 */
	public void add(DatalogSequence p) {
//		if (p.isRecursive())
			_programs.add(p);
//		else
//			_programs.addAll(p.getSequence());
	}

	/*
	public void addAll(Collection<Datalog> p) {
		for (Datalog o : p)
			if (o instanceof DatalogSequence)
				add((DatalogSequence)o);
			else if (o instanceof DatalogProgram)
				add((DatalogProgram)o);
			else
				throw new RuntimeException("Incompatible object in collection!");
	}*/
	
	public void addAll(Collection<Datalog> p) {
		for (Datalog o : p)
			if (o instanceof DatalogSequence)
				add((DatalogSequence)o);
			else if (o instanceof DatalogProgram)
				add((DatalogProgram)o);
			else
				throw new RuntimeException("Incompatible object in collection!");
	}
	
	
	public Datalog get(int i) {
		return _programs.get(i);
	}
	
	public int size() {
		return _programs.size();
	}
	
	public List<Datalog> getSequence() {
		return _programs;
	}
	
	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		
		int cnt = 0;
		
		if(isRecursive())
			b.append("Recursive Sequence { \n");
		else
			b.append("Non-Recursive Sequence { \n");
			
		for (Object o : _programs) {
			if (cnt++ > 0)
				b.append("\n");
			
			b.append(o.toString());
		}
		
		b.append("} END Sequence \n");
		return new String(b);
	}

	public void printString() {
		if(isRecursive())
			Debug.println("Recursive Sequence {");
		else
			Debug.println("Non-Recursive Sequence {");
			
		for (Object o : _programs) {
			if (o instanceof DatalogProgram)
				((DatalogProgram)o).printString();
			else if (o instanceof DatalogSequence)
				((DatalogSequence)o).printString();
			else
				Debug.println(o.toString());
		}
		Debug.println("} END Sequence \n");
	}
	
	@Override
	public Element serialize(Document document) {
		Element e = super.serialize(document);
		e.setAttribute("type", "datalogSequence");
		e.setAttribute("isRecursive", Boolean.toString(_isRecursive));
		for (Datalog program : _programs) {
			e.appendChild(program.serialize(document));
		}
		return e;
	}
	
	/**
	 * Returns the {@code DatalogSequence} represented by {@code datalog}.
	 * 
	 * @param datalog an {@code Element} produced by {@code serialize(Document)}
	 * @param system 
	 * @return the {@code DatalogSequence} represented by {@code datalog}
	 * @throws XMLParseException 
	 */
	public static DatalogSequence deserialize(Element datalog, OrchestraSystem system) throws XMLParseException {
		boolean countForFixpoint = DomUtils.getBooleanAttribute(datalog,
				"countForFixpoint");
		boolean measureExecTime = DomUtils.getBooleanAttribute(datalog,
				"measureExecTime");
		boolean isRecursive = DomUtils.getBooleanAttribute(datalog,
				"isRecursive");
		List<Datalog> datalogs = OrchestraUtil.newArrayList();
		List<Element> datalogElements = DomUtils.getChildElements(datalog);
		for (Element datalogElement : datalogElements) {
			datalogs.add(Datalog.deserialize(datalogElement, system));
		}
		DatalogSequence sequence = new DatalogSequence(isRecursive, datalogs,
				countForFixpoint, measureExecTime);
		return sequence;
	}

	/**
	 * Returns a XML representation of the code this datalog sequence transforms
	 * into.
	 * 
	 * @param doc only used for {@code Element} creation
	 * @return a XML representation of the code this datalog sequence transforms
	 *         into
	 */
	@Override
	public List<Element> serializeAsCode(Document doc) {
		List<Element> result = newArrayList();
		for (Datalog datalog : _programs) {
			if (datalog instanceof DatalogProgram) {
				DatalogProgram program = (DatalogProgram) datalog;
				result.addAll(program.serializeAsCode(doc));
			} else if (datalog instanceof DatalogSequence) {
				DatalogSequence sequence = (DatalogSequence) datalog;
				List<Datalog> subDatalogs = sequence.getSequence();
				for (Datalog subDatalog : subDatalogs) {
					result.addAll(subDatalog.serializeAsCode(doc));
				}
			}
		}
		return result;
	}
}
