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

//import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
//import edu.upenn.cis.orchestra.engine.IDb;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.exchange.RuleQuery;

/**
 * 
 * @author gkarvoun
 *
 */
public class SingleRuleDatalogProgram extends NonRecursiveDatalogProgram {
	//protected List<PreparedStatement> _stmts;
	protected RuleQuery _stmts;
	protected String _queryString = new String("");
	protected boolean preparedFlag = false;
	
	public SingleRuleDatalogProgram(List<Rule> r, boolean c4f, String desc){
		super(r, c4f, desc);
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(List<Rule> r, String desc){
		super(r, desc);
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(Rule r, String desc){
		super(null, desc);
		List<Rule> l = new ArrayList<Rule>();
		l.add(r);
		_rules = l;
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(Rule r, boolean c4f, String desc){
		super(null, desc);
		List<Rule> l = new ArrayList<Rule>();
		l.add(r);
		_rules = l;
		_stmts=null;
		_c4f = c4f;
	}
	
	/*
	public boolean isPrepared(){
//		return(_stmts != null);
		return preparedFlag;
	}
	
	public void initialize(RuleQuery q, Map<ScField, String> typesMap) {
		if (_stmts != null)
			return;
		
		StringBuffer qString = new StringBuffer();
		
		List<String> qs = getQuery(typesMap);
		
		for (String query : qs){
			q.add(query);
			qString.append("\n");
			qString.append(query);
		}

		_queryString = new String(qString);
		_stmts = q;
	}
	
	public int evaluate() {
		return _stmts.evaluateSelf();
	}
	
	//public List<PreparedStatement> statements(){
	public RuleQuery statements() {
		return _stmts;
	}
	
	public List<String> getQuery(Map<ScField, String> typesMap) {
		Rule rule = (Rule)getRules().get(0);
		List<String> queries = rule.toUpdate(typesMap);
		
		return queries;//new String(qString);
	}
	
	public void prepare() {
		preparedFlag = true;
		try {
			_stmts.prepare();
		} catch (Exception e) {
			throw new RuntimeException("Unable to prepare:\n" + e.getStackTrace());
		}
		
		//public List<PreparedStatement> prepare(
			//SqlDb con, Map<ScField, String> typesMap){
	}*/
	
	@Override
	public String toString ()
	{
		if(_queryString != null){
			return _queryString;
		}else{
			StringBuffer buffer = new StringBuffer ();
			Rule r = (Rule)getRules().get(0);
//			Debug.println("EVALUATE: " + r.toString());

			buffer.append("\n" + r.toString() + "\n");

			return buffer.toString();
		}
	}
	
	public String toQueryString ()
	{
		if(_queryString != null){
			return _queryString;
		}else{
			StringBuffer buffer = new StringBuffer ();
			Rule r = (Rule)getRules().get(0);
//			Debug.println("EVALUATE: " + r.toString());
			for(String query : r.toUpdate(0)){
				buffer.append("\n" + query);
			}
			buffer.append("\n");
			
			return buffer.toString();
		}
	}

}
