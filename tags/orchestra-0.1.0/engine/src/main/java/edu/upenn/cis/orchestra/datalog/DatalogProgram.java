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
import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeVerboseRules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * 
 * @author gkarvoun
 *
 */
public abstract class DatalogProgram extends Datalog {
	protected RuleQuery _stmts;
	String _queryString;
	boolean preparedFlag = false;

	public List<Rule> _rules;
	public Map<Rule, List<String>> _bodyTables;
	public Map<Rule, List<String>> _headTables;
//	public boolean _c4f;
//	public boolean _measureExecTime;
	
	public DatalogProgram(List<Rule> r, boolean c4f){
		_rules = r;
		_c4f = c4f;
		_measureExecTime = false;
		_bodyTables = new HashMap<Rule, List<String>>();
		_headTables = new HashMap<Rule, List<String>>();
	}

	public DatalogProgram(List<Rule> r, boolean c4f, boolean t){
		_rules = r;
		_c4f = c4f;
		_measureExecTime = t;
		_bodyTables = new HashMap<Rule, List<String>>();
		_headTables = new HashMap<Rule, List<String>>();
	}
	
	public DatalogProgram(List<Rule> r){
		_rules = r;
		_c4f = true;
		_bodyTables = new HashMap<Rule, List<String>>();
		_headTables = new HashMap<Rule, List<String>>();
	}
	
	public List<Rule> getRules(){
		return _rules;
	}
	
	public Map<Rule, List<String>>getBodyTables() {
		return _bodyTables;
	}

	public Map<Rule, List<String>>getHeadTables() {
		return _bodyTables;
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
	
	public void omitFromCount() {
		_c4f = false;
	}

//	public String toString(){
//		StringBuffer buf = new StringBuffer();
//		if(this instanceof RecursiveDatalogProgram){
//			buf.append("Recursive: ");
//		}else{
//			buf.append("Non-Recursive: ");
//		}
//		int count = 0;
//		for (Rule r : getRules()){
//			if (count++ > 0)
//				buf.append("\n");
//			
//			buf.append(r.toString());
//		}
//		return(buf.toString());
//	}
	
	public String toQueryString ()
	{
		if(_queryString != null){
			return _queryString;
		}else{
			StringBuffer buffer = new StringBuffer ();
			for (Rule r : getRules()) {
				for(String query : r.toUpdate(0)){
					buffer.append("\n" + query);
				}
				buffer.append("\n");
			}
			
			return buffer.toString();
		}
	}
	
	@Override
	public String toString ()
	{
		StringBuffer buffer = new StringBuffer ();
		for (Rule r : getRules()) {
			buffer.append("\n" + r.toString() + "\n");
		}

		return buffer.toString();
	}
	
	public void printString() {
		if(this instanceof RecursiveDatalogProgram){
			Debug.println("Recursive{ ");
		}else{
			Debug.println("Non-Recursive{ ");
		}
//		int count = 0;
		for (Rule r : getRules()){
//			if (count++ > 0)
//				Debug.print("\n");
			
			r.printString();
		}
		if(this instanceof RecursiveDatalogProgram){
			edu.upenn.cis.orchestra.Debug.println("} END Recursive");
		}else{
			Debug.println("} END Non-Recursive");
		}
	}

	@Override
	public Element serialize(Document document) {
		Element e = super.serialize(document);
		e.setAttribute("type", "datalogProgram");
		for (Rule r : getRules()) {
			e.appendChild(r.serializeVerbose(document));
		}
		return e;
	}
	
	@Override
	public List<Element> serializeAsCode(Document doc) {
		List<Element> result = newArrayList();
		for (Rule rule : _rules) {
			Element code = rule.serializeAsCode(doc);
			result.add(code);
		}
		return result;
	}

	/**
	 * Returns the {@code DatalogProgram} represented by {@code datalog}.
	 * 
	 * @param datalog a 'datalogProgram' {@code Element} produced by {@code
	 *            serialize(Document)}
	 * @param system 
	 * @return the {@code DatalogProgram} represented by {@code datalog}
	 * @throws XMLParseException
	 */
	public static DatalogProgram deserialize(Element datalog, OrchestraSystem system)
			throws XMLParseException {
		boolean countForFixpoint = DomUtils.getBooleanAttribute(datalog,
				"countForFixpoint");
		boolean measureExecTime = DomUtils.getBooleanAttribute(datalog,
				"measureExecTime");
		String programType = datalog.getAttribute("programType");
		List<Rule> rules = deserializeVerboseRules(datalog, system);
		DatalogProgram program = null;
		if ("nonRecursiveDatalogProgram".equals(programType)) {
			program = new NonRecursiveDatalogProgram(rules, countForFixpoint);
			program.setMeasureExecTime(measureExecTime);
		} else if ("recursiveDatalogProgram".equals(programType)) {
			program = new RecursiveDatalogProgram(rules, countForFixpoint);
			program.setMeasureExecTime(measureExecTime);
		} else if ("".equals(programType)) {
			throw new XMLParseException(
					"Missing 'programType' attribute of 'datalog' element.");
		} else {
			throw new XMLParseException(
					"Unknown value for 'programType' of 'datalog': " + programType + ".");
		}
		return program;

	}
	
	public void initialize(RuleQuery q, int curIterCnt, boolean recomputeQueries) {
//		For (unprepared) stratified we need to generate 
//		different SQL queries for every fixpoint iteration
		if(!recomputeQueries){
			if (_stmts != null)
				return;
		}
		
		StringBuffer qString = new StringBuffer();
		List<List<Integer>> params = new ArrayList<List<Integer>>();

		_bodyTables = getRulesBodyTables();
		_headTables = getRulesHeadTables();
		
		List<String> qs = getQuery(curIterCnt, params);
		
		if(qs.size() != params.size()){
			System.out.println("DIFFERENT NUM QUERIES VS. NUM PARAMS");
		}
		
		for(int i = 0; i < qs.size(); i++){
			String query = qs.get(i);
			List<Integer> qparams = params.get(i);
//		for (String query : qs){

			if(Config.getStratified())
				q.addPrepared(query, qparams);
			else
				q.add(query);
//			q.add(query);
//			q.setPreparedParams(qparams);
			
			qString.append("\n");
			qString.append(query);
		}

		_queryString = new String(qString);
		_stmts = q;
	}
	
	public int evaluate(int curIterCnt) {
		Debug.println("QQQQQ: " + _queryString);
		return _stmts.evaluateSelf(_queryString, curIterCnt, _rules, _bodyTables, _headTables);
	}
	
//	public List<PreparedStatement> statements(){
	public RuleQuery statements() {
		return _stmts;
	}
	public Map<Rule, List<String>> getRulesBodyTables() {
		Map<Rule, List<String>> result = new HashMap<Rule, List<String>>();
		 
		for (Rule rule : getRules()) {
			result.put(rule, rule.getBodyTables());
		}
		return result;
	}

	public Map<Rule, List<String>> getRulesHeadTables() {
		Map<Rule, List<String>> result = new HashMap<Rule, List<String>>();
		 
		for (Rule rule : getRules()) {
			result.put(rule, rule.getHeadTables());
		}
		return result;
	}
	
	public List<String> getQuery(int curIterCnt, List<List<Integer>> params) {
		List<String> queries = new ArrayList<String>();

//		Debug.println("Query...");
//		Rule last = null;
		HashSet<Rule> done = new HashSet<Rule>();
//		List<Integer> preparedParams = new ArrayList<Integer>();
		
		for (Rule rule : getRules()) {
			/*if (done.contains(rule))
				continue;
			else
				Debug.println("R: " + rule);*/
			
//			if (rule.deleteFromHead() && !rule.clearNcopy()) {
//				// TODO: See if we can merge with the previous query
//				queries = rule.toUpdate();
//			} else {
//				queries = rule.toUpdate(queries, curIterCnt, preparedParams);
				queries = rule.toUpdate(queries, curIterCnt, params);
//			}
//			last = rule;
//			params.add(preparedParams);
			done.add(rule);
		}
		
//		for (String q: queries)
//			Debug.println(q);
		
		return queries;//new String(qString);
	}
	
	public void prepare() {
		preparedFlag = true;
		try {
			_stmts.prepare();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to prepare:\n" + e.getStackTrace());
		}
	}
	
	public boolean isPrepared(){
//		return(_stmts != null);
		return preparedFlag;
	}

}
