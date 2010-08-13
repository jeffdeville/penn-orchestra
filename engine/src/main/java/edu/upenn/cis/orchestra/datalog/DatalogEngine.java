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

import java.util.Calendar;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * 
 * @author gkarvoun
 *
 */
public class DatalogEngine {
	protected String slowestQuery;
	protected long slowestQueryTime = 0;
//	protected int globalIterCnt;

//	protected CreateProvenanceStorage _provenancePrep;
	
	public IDb _sql;
	
//	protected int queryCnt = 0;	
//	protected int transactionCnt = 0;
	
	public DatalogEngine(
			IDb database){
		_sql = database;
//		globalIterCnt = 0;
	}
	
	

	public void commitAndReset() throws Exception{
		if (Config.getApply() && !Config.getAutocommit())
			commit();

		resetCounters();	
	}
	
	public void commit() throws Exception {
		_sql.commit();
	}

	public void resetCounters(){
		_sql.resetCounters();
		slowestQueryTime = 0;
		slowestQuery = null;
	}
	
	public void disconnect() throws Exception{
		_sql.commit();
		_sql.disconnect();
	}

	/**
	 * Generate the SQL or other underlying query for each rule in the program.
	 * TODO: integrate this into the evaluatePrograms() code path so we can separate
	 *       plan/query generation from execution.
	 * 
	 * @param prog
	 */
	public void generatePhysicalQuery(DatalogSequence prog) {
		for (Datalog p: prog.getSequence()) {
			if (p instanceof DatalogSequence) {
				generatePhysicalQuery((DatalogSequence)p);
			} else if (p instanceof RecursiveDatalogProgram || (p instanceof NonRecursiveDatalogProgram)){
				DatalogProgram rule = (DatalogProgram)p;
				rule.initialize(_sql.generateQuery(), 0, true);

				// Prepare the statement?
				if (Config.getPrepare() && !rule.isPrepared())
					rule.prepare();
				
			} else
				throw new RuntimeException("Unexpected type in datalog sequence");
		}
	}
	
	/*
	public Connection connect() throws Exception{
		return _sql.connect();
	}*/
	
	public int evaluatePrograms(DatalogSequence prog){
		return evaluatePrograms(prog, 1);
	}
	
	public int evaluatePrograms(DatalogSequence prog, boolean recomputeQueries){
		return evaluatePrograms(prog, 1, recomputeQueries);
	}
	
	public int evaluatePrograms(DatalogSequence prog, int iterCnt){
		return evaluatePrograms(prog, iterCnt, false);
	}
	
	public int evaluatePrograms(DatalogSequence prog, int iterCnt, boolean recomputeQueries) {
		int ret = 0;
		int localRet;
		
		if(prog.isRecursive())
			iterCnt = 0;
		
//		Calendar before;
//		Calendar after;
//		long time;
//		before = Calendar.getInstance();
//
//		after = Calendar.getInstance();
//		time = after.getTimeInMillis() - before.getTimeInMillis();
//		Debug.println("INSERTION PREP TIME: " + time + " msec");		

		do {
			if(prog.isRecursive())
				iterCnt++;
			
			localRet = 0;
			for (Datalog p: prog.getSequence()) {
				Calendar before = Calendar.getInstance();
				
				if (p instanceof DatalogSequence) {
					if(((DatalogSequence)p).count4fixpoint()){
						int num = evaluatePrograms((DatalogSequence)p, iterCnt, recomputeQueries);
						if(prog.isRecursive())
							Debug.println("Recursive sequence " + prog.hashCode() + " - SUBPROGRAM RETURNED COUNT: " + num);
						localRet += num;
					} else
						evaluatePrograms((DatalogSequence)p, iterCnt, recomputeQueries);
				} else if (p instanceof DatalogProgram) {
					if(((DatalogProgram)p).count4fixpoint()) {
						int num = evaluateProgram((DatalogProgram)p, iterCnt, recomputeQueries);
						if(prog.isRecursive())
							Debug.println("Recursive sequence - SUBPROGRAM RETURNED COUNT: " + num);
						localRet += num;
					} else
						evaluateProgram((DatalogProgram)p, iterCnt, recomputeQueries);
				} else
					throw new RuntimeException("Unexpected type in datalog sequence");
				
				Calendar after = Calendar.getInstance();

				if(p.measureExecTime()){
					long time = after.getTimeInMillis() - before.getTimeInMillis();
					Debug.println("(SUB)PROGRAM EXECUTION TIME: " + time + " msec");
				}
				
//				_sql.runstats();
			}
			ret += localRet;
			if(prog.isRecursive())
				Debug.println("Recursive sequence - LAST ITERATION (" + iterCnt + ") RETURNED COUNT: " + localRet);
		} while (localRet != 0 && prog.isRecursive());
		if(prog.count4fixpoint())
			return ret;
		else
			return 0;
	}
	
	
	public int evaluateProgram(DatalogProgram prog, int iterCnt){
		return evaluateProgram(prog, iterCnt, false);
	}
	
	public int evaluateProgram(DatalogProgram prog, int iterCnt, boolean recomputeQueries){
		int ret = 0;
		try{
			//Debug.println("Evaluating " + prog.toString());

			if(prog instanceof RecursiveDatalogProgram){
				computeFixpoint(prog);
				ret = 0;
				//				ret = computeFixpoint(prog);
			} else if (prog instanceof NonRecursiveDatalogProgram){
				ret = evaluateProgramNoRecursion(prog, iterCnt);
			} else {
				throw new RuntimeException("Unexpected rule type");
			}
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

		if(prog.count4fixpoint())
			return ret;
		else
			return 0;
	}
	
	public int computeFixpoint(DatalogProgram prog){
		return computeFixpoint(prog, false);
	}
	
	public int computeFixpoint(DatalogProgram prog, boolean recomputeQueries){
		int totalNum = 0;
		int num = 0;
		//		int iterCnt = globalIterCnt;
		int iterCnt = 0;

		//		Debug.println("\nBEGIN FIXPOINT");
		//Calendar before = Calendar.getInstance();

		try {
			do { 
				iterCnt++;
				num = evaluateProgramNoRecursion(prog, iterCnt, recomputeQueries);
				/*
				for (Object r : rules) {
					if(r instanceof SingleRuleDatalogProgram){
						num += evaluateSingleRule((SingleRuleDatalogProgram)r, typesMap);
					}else if(r instanceof DatalogProgram){
						num += evaluateProgram((DatalogProgram)r, typesMap);
					} 
				}
				 */
				totalNum += num;

				if(_sql instanceof SqlDb)
					((SqlDb)_sql).gatherStats();
				Debug.println("LAST ITERATION (" + iterCnt + ") COUNT: " + num + "\n");
				Debug.println("");
			}
			while(num > 0);
			//			Calendar after = Calendar.getInstance();
			//			Debug.println("END FIXPOINT - " + iterCnt + " ITERATIONS\n");
			//			long time = after.getTimeInMillis() - before.getTimeInMillis();
			//			Debug.println("FIXPOINT TIME: " + time + "msec\n");
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
		return totalNum;
	}

	public int evaluateProgramNoRecursion(DatalogProgram rule, int curIterCnt)
	{
		return evaluateProgramNoRecursion(rule, curIterCnt, false);
	}
	
	public int evaluateProgramNoRecursion(DatalogProgram rule, int curIterCnt, boolean recomputeQueries)
	{
		int num = 0;

		if(rule.getRules().size() == 0)
			return 0;

		try {
			rule.initialize(_sql.generateQuery(), curIterCnt, recomputeQueries);

			// Prepare the statement?
			if (Config.getPrepare() && !rule.isPrepared())
				rule.prepare();

			//			Debug.println("EVALUATE: " + rule.toString());
			num = rule.evaluate(curIterCnt);

			//	        transactionCnt+=num;
			//	        queryCnt += rule.statements().size();

			//			Debug.println("QUERY COUNT: " + queryCnt);
			//			Debug.println("TUPLE COUNT: " + transactionCnt);

			Rule r = (Rule)rule.getRules().get(0);
			if(r.getHead().isNeg() || !rule.count4fixpoint()){
				Debug.println("DELETE RETURNED COUNT: " + num);
				//				commit();
				return 0;
			}else{
				//if(num != 0){
				//Debug.println("EVALUATE: " + query + ";");
				Debug.println("INSERT RETURNED COUNT: " + num);
				//}
				//				commit();
				return num;
			}
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}


		return num;
	}

	public long logTime(){
		return _sql.logTime();
	}
	
	public long emptyTime(){
		return _sql.emptyTime();
	}
}
