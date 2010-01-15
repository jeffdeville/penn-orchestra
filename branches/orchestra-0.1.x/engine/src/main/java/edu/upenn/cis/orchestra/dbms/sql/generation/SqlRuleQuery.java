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
package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.exchange.RuleQuery;

/**
 * @author zives, gkarvoun
 *
 */
public class SqlRuleQuery extends RuleQuery {
	List<List<Integer>> _preparedParams;

	public SqlRuleQuery(SqlDb db) {
		super(db);
	}

	@Override
	public void setPreparedParams(List<List<Integer>> l){
		_preparedParams = l;
	}

	@Override
	protected SqlDb getDatabase() {
		return (SqlDb)super.getDatabase();
	}

	/**
	 * Add another statement
	 * 
	 * @param statement
	 */
//	public void add(String statement) {
//	// Optimization ...
//	_statements.add(statement + " optimize for " + Config.EST_TABLE_SIZE + " rows");
//	}


	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.engine.RuleQuery#evaluateSelf(edu.upenn.cis.orchestra.engine.IDb)
	 */
	@Override
	public int evaluateSelf(String queryString, int curIterCnt, List<Rule> rules, Map<Rule, List<String>> bodyTables, Map<Rule, List<String>> headTables) {
		int st = 0;
		try {
			int num = 0;
			if (isPrepared() && Config.getApply()) {
				int k = 0;
				int j = 0;
				int l = 0;

				for (PreparedStatement stmt : _prepared){
//					String [] queries = queryString.split("\n");
//					for(String q : queries){
//					if(!("").equals(q)){
//					Debug.println("PLAN: EXPLAIN PLAN FOR " + q);
//					boolean foo = getDatabase().evaluate("EXPLAIN PLAN FOR " + q);
//					}
//					}
//					Debug.println("QUERY: " + stmt);

					try{
						boolean execute = true;
						if(!rules.get(j).getDeleteFromHead() && !rules.get(j).clearNcopy()){
							SqlEmptyTables empty = new SqlEmptyTables(bodyTables.get(rules.get(j)));

							if(empty.emptyTables(getDatabase())){
								execute = false;
							}
							j++;
						}else if(rules.get(j).clearNcopy()){
							l++;
							if(l > 2){
								j++;
								l = 0;
							}
						}else{
							j++;
						}
						if(execute){
							if(_preparedParams.size() <= k)
								num += getDatabase().evaluatePrepared(stmt, curIterCnt, null);
							else
								num += getDatabase().evaluatePrepared(stmt, curIterCnt, _preparedParams.get(k));
						}

					}catch(Exception e){
						Debug.println("Why is this happening?");
					}
					k++;
				}
			} else {
				if (Config.getApply() && _statements.size() == 1) {
					for (String stmt : _statements) {
						Debug.println("QUERY: " + stmt);
						if(stmt.startsWith("ALTER")){
							getDatabase().evaluate(stmt);
						}else{
							num += getDatabase().evaluateUpdate(stmt);
						}
					}
				} else if (Config.getApply()) {
					if(Config.getBatch()){
						boolean batchNotEmpty = false;
						for (String stmt : _statements) {
							if(stmt.startsWith("DROP") || stmt.startsWith("RENAME") || stmt.startsWith("CREATE") || stmt.startsWith("ALTER")){
								Debug.println("ESCAPE FROM BATCH: " + stmt);
								getDatabase().evaluateUpdate(stmt);
							}else{
								Debug.println("ADD TO BATCH: " + stmt);
								getDatabase().addToBatch(stmt);
								batchNotEmpty = true;
							}
							st++;
						}
						if(batchNotEmpty)
							num += getDatabase().evaluateBatch();
					}else{
						for (String stmt : _statements) {
							Debug.println("SQL: " + stmt);

							int foo = getDatabase().evaluateUpdate(stmt);
							num += foo;
							st++;
						}
					}
				}
			}

//			Run statistics on tables that have changed by the execution of this program
			List<String> headTbls = new ArrayList<String>();
			for(Rule r : rules){
				for(String s : headTables.get(r))
					if(!headTbls.contains(s))
						headTbls.add(s);
			}
			getDatabase().runstats(headTbls);
			return num;
		} catch (Exception e) {
			System.err.println("Error with:");
			for (int i = 0; i <= st; i++)
				System.err.println(_statements.get(st));
			e.printStackTrace();
			throw new RuntimeException("Aborting: " + e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.engine.RuleQuery#prepare()
	 */
	@Override
	public boolean prepare() throws SQLException {
		if(!isPrepared()){
			if(_prepared == null)
				_prepared = new ArrayList<PreparedStatement>();


			for (String stmt : _statements)
				_prepared.add(getDatabase().createPrepared(stmt));

			_isPrepared = true;
		}
		return false;
	}

	List<PreparedStatement> _prepared;

	public void addPrepared(String statement, List<Integer> params){
		_statements.add(statement);
		if(_preparedParams == null)
			_preparedParams = new ArrayList<List<Integer>>();

		_preparedParams.add(params);

	}

	public void cleanupPrepared() {
		try {
			if(isPrepared() && _prepared != null) {
				for(PreparedStatement p : _prepared) {
					p.close();
				}
				_isPrepared = false;
			}
		}catch(SQLException e){
			System.out.println("Exception: prepared statement close failed");
		}
	}


}
