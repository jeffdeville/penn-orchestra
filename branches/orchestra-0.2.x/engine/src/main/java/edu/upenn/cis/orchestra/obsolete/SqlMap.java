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
package edu.upenn.cis.orchestra.obsolete;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

/**
 * Mainline to run the DB2 Orchestra mapping application
 * 
 * @author zives, gkarvoun
 *
 */
public class SqlMap {
	public static void main(String[] args) {
		Config.parseCommandLine(args);
		Config.dumpParams(System.out);

		if(Config.getPrepare() && Config.getUnion() && Config.getStratified()){
			throw new RuntimeException("Current Implementation of stratified deletions" +
					" with prepared statements maps one list of parameters to every query" +
					" and thus doesn't work with union");
		}
		
		try {
			// NOTE:  a general user will actually call open, importUpdates, etc.
			// but instead we are calling lower-level routines right now, to test.

			RepositorySchemaDAO dao = new FlatFileRepositoryDAO(Config.getSchemaFile());
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = SqlEngine.getNamesOfAllTables(system, false, false, true);
//			List<String> tables = SqlEngine.getNamesOfAllTables(system, true, true, true);
			
	        SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			if (Config.getApply()) {
				d.connect();
			}
			SqlEngine tcd = new SqlEngine(d, //null, 
					system);
			
			Peer p = system.getPeers().iterator().next();
			
			try{
				if(Config.getApply() && !Config.getBoolean("scalability2"))
					createDatabase(Config.getProperty("schema"), d);

				Debug.println("+++ Basic System Schema +++");
				for (String t: tables)
					Debug.print(t + "\t");
				Debug.println("");

				Debug.println("+++ Mappings +++");
				List<Mapping> mappings = system.getAllSystemMappings(true);
				for (Mapping s: mappings)
					Debug.println(s.toString());

				Debug.println("+++ Inverted/Composed Mappings with Provenance Relations +++");
				List<Rule> ruleSet = tcd.computeTranslationRules();

//				for(RelationContext rel : tcd.getState().getIdbs()){
//					List<RelationContext> p = tcd.getState().provenanceTablesForRelation(rel);
//					
//					System.out.println("Provenance tables for " + rel + ": " + p);
//				}
				
				for (Rule r: ruleSet)
					Debug.println(r.toString());

				boolean origApply = true;
				if(Config.getBoolean("scalability2")){
					origApply = Config.getApply();
					Config.setApply(false);
				}
				
				if(Config.getApply())
					tcd.migrate();
				
				if(Config.getBoolean("scalability2")){
					Config.setApply(origApply);
				}
				
				//DeltaRules ourRules = 
				tcd.computeDeltaRules();


				Debug.println("+++ Insertion Rules +++");
				List<DatalogSequence> insRules = tcd.getIncrementalInsertionProgram();
				insRules.get(0).printString();
				insRules.get(1).printString();
				insRules.get(2).printString();
				
				List<DatalogSequence> delRules = tcd.getIncrementalDeletionProgram();
				Debug.println("+++ Deletion Rules +++");
				delRules.get(0).printString();
				delRules.get(1).printString();
				delRules.get(2).printString();
				
				
				List<String> newTables =  SqlEngine.getNamesOfAllTablesFromDeltas(/*ourRules,*/ system, true, true, true);
				d.setAllTables(newTables);

				if(Config.getBoolean("scalability")){
					runScalability(tcd, d, p);
				}else if(Config.getBoolean("scalability1")){
					runScalability1(tcd, d, p);
				}else if(Config.getBoolean("scalability2")){
					runScalability2(tcd, d, p);
				}else if(Config.getBoolean("nullscycles")){
					runAllnoMigrateInsOnly(tcd, p);
				}else if(Config.getDebug()){
					runOnce(tcd, p);
				}else{
//					runAllmigrateEveryTime(system, tcd, d, tables);
					runAllnoMigrate(tcd, d, p);
				}			
				
				tcd.finalize();
//				tcd.clean();
				if (Config.getApply()) {
					d.disconnect();
				}

			} catch (Exception e) {
				e.printStackTrace();
				tcd.clean();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int recno = 0;

	public static void createDatabase(String schemaName, SqlDb db) {
		File dir = new File(Config.getProperty("workdir"));

		Calendar before = Calendar.getInstance();

		if (Config.isDB2() || Config.isOracle())
			db.evaluateFromShell(schemaName + ".create", dir, false);
		else if (Config.isHsql()){
			try {
				BufferedReader r = new BufferedReader(new FileReader(dir + "/" + schemaName + ".create"));
	
				db.evaluate("CREATE SCHEMA " + Config.getProperty("sqlschema") + " AUTHORIZATION DBA");
				String ln = r.readLine();
				String sql = "";
				boolean startReading = false;
				while (ln != null) {
					if (ln.contains("CREATE")) {
						startReading = true;
						//db.evaluate(ln);
					}
					if (startReading)
						sql = sql + ln;
					
					if (ln.contains(";")) {
						startReading = false;
						sql = sql.replace("NOT LOGGED INITIALLY", "");
						sql = sql.replace("CREATE TABLE ", "CREATE TABLE " + Config.getProperty("sqlschema") + ".");
						db.evaluate(sql);
						sql = "";
					}
					
					ln = r.readLine();
				}
				
				r.close();
				
			} catch (java.io.IOException e) {
				e.printStackTrace();
			}
		} else
			throw new RuntimeException("Unimplemented DBMS!");
		
		Calendar after = Calendar.getInstance();
		long time = (after.getTimeInMillis() - before.getTimeInMillis());
        System.out.println("EXP: CREATE DB TIME: " + time + "msec");
	}

	public static void setupDb(OrchestraSystem system, SqlDb d, SqlEngine tcd, List<String> tables) throws Exception{
		if(Config.getApply())
			createDatabase(Config.getProperty("schema"), d);
		
		tcd.migrate();

		tcd.computeDeltaRules();	
	}
	
	public static void run(SqlEngine tcd, Peer p, int n) throws Exception{
		for(int j = 0; j < n; j++){
			tcd.computeDeltaRules();
			
			tcd.importUpdates(new FileDb(Config.getWorkloadPrefix(), Config.getImportExtension()));

			tcd.mapUpdates(recno, ++recno, p, true);

			if (Config.getReset())
				tcd.reset();
		}
		
	}
	
	public static void runNonIncremental(SqlEngine tcd, Peer p, int n) throws Exception{

		Config.setNonIncremental(true);
		
		for(int j = 0; j < n; j++){
			tcd.computeDeltaRules();
			
			tcd.importUpdates(new FileDb(Config.getWorkloadPrefix(), Config.getImportExtension()));
			
			tcd.mapUpdates(recno, ++recno, p, true);

			if (Config.getReset())
				tcd.reset();
			
			tcd.resetConnection();
		}

	}
	
//	public static void runNonIncremental(SqlEngine tcd, int n) throws Exception{
//
//		Config.DO_NON_INCREMENTAL = true;
//		
//		run(tcd, n);
//	}
	
	public static void runIncremental(SqlEngine tcd, Peer p, int n) throws Exception{

		Config.setNonIncremental(false);
//		tcd.getDeltaRules();
		
		run(tcd, p, n);
	}
	
	public static void calcAvgTimes(){	
		int insMin = 0;
		int insMax = 1;
		int delMin = 0;
		int delMax = 1;
		insMin = minIndex(SqlEngine.insTimes);
		insMax = maxIndex(SqlEngine.insTimes);
		delMin = minIndex(SqlEngine.delTimes);
		delMax = maxIndex(SqlEngine.delTimes);

	
		System.out.println("EXP: AVG INS TIME: " + avg(SqlEngine.insTimes, insMin, insMax) + "msec");						
		System.out.println("EXP: AVG DEL TIME: " + avg(SqlEngine.delTimes, delMin, delMax) + "msec");	
	
		System.out.println("================");
	}
	
	public static int minIndex(List<Long> l){
		Long minVal = Long.MAX_VALUE;
		int min = 0;
		
		for(int i = 0; i < l.size(); i++){
			if(l.get(i) < minVal){
				min = i;
				minVal = l.get(i);
			}
		}
		return min;
	}
	
	public static int maxIndex(List<Long> l){
		Long maxVal = Long.MIN_VALUE;
		int max = 0;
		
		for(int i = 0; i < l.size(); i++){
			if(l.get(i) > maxVal){
				max = i;
				maxVal = l.get(i);
			}
		}
		return max;
	}
	
	public static long avg(List<Long> l, int min, int max){ 
		long avg = 0;
		StringBuffer buf = new StringBuffer();
		
		buf.append("ALL NUMBERS (msec): ");
		for(int i = 0; i < l.size(); i++){
			if(i < l.size()-1)
				buf.append(l.get(i) + ", ");
			else
				buf.append(l.get(i));
			
			if(i != min && i != max){
				avg += l.get(i);
			}
		}
		System.out.println(buf.toString());
		return (avg/((long)l.size()-2));
	}

	public static void runAllmigrateEveryTime(OrchestraSystem system, SqlEngine tcd, SqlDb d, List<String> tables) throws Exception{
		tcd.cleanKeepConn();

		Peer p = system.getPeers().iterator().next();
//		int k = 10; int l = 1;
		for(int k = 1000; k <= 10000; k*=10){
			for(int l = k/10; l <= k/2; l+=k/10){
				Config.setWorkloadPrefix(Config.getTestSchemaName() + "-"+k+"i"+l+"d");

				System.out.println("================");
				System.out.println("SCHEMA: " + Config.getWorkloadPrefix());
				System.out.println("================");

				SqlEngine.insTimes = new ArrayList<Long>();
				SqlEngine.delTimes = new ArrayList<Long>();

				setupDb(system, d, tcd, tables);
				runNonIncremental(tcd, p, 5);

//				for(int m = 0; m < 5; m++){
//				setupDb(system, d, tcd, tables);
//				runNonIncremental(tcd, 1);
//				tcd.cleanKeepConn();
//				}
				calcAvgTimes();
				tcd.cleanKeepConn();

				SqlEngine.insTimes = new ArrayList<Long>();
				SqlEngine.delTimes = new ArrayList<Long>();

				for(int m = 0; m < 5; m++){
					setupDb(system, d, tcd, tables);
					runIncremental(tcd, p, 1);
					tcd.cleanKeepConn();
				}
				calcAvgTimes();
			}
		}

	}

	public static void runScalabilityInsDel(SqlEngine tcd, SqlDb d, Peer p, int ins, int del) throws Exception {
		Config.setWorkloadPrefix(Config.getTestSchemaName() + "-" + ins + "i" + del + "d");

		SqlEngine.insTimes = new ArrayList<Long>();
		SqlEngine.delTimes = new ArrayList<Long>();

		for(int i = 0; i < 2; i ++){
			tcd.computeDeltaRules();

			tcd.importUpdates(new FileDb(Config.getWorkloadPrefix(), Config.getImportExtension()));

			tcd.mapUpdates(recno, ++recno, p, false);
			
			d.resetCounters();
			tcd.commit();
			d.resetConnections();
		}
//		calcAvgTimes();
	}
	
	public static void runScalability(SqlEngine tcd, SqlDb d, Peer p) throws Exception {
//		Config.FULL_TEST_NAME = Config.TEST_SCHEMA_NAME + "-10000i0d";
		Config.setWorkloadPrefix(Config.getTestSchemaName() + "-10i0d");
		
		// The insert and delete options were removed.
		//Config.setInsert(true);
		//Config.setDelete(false);
		Config.setNonIncremental(false);

		tcd.importUpdates(new FileDb(Config.getWorkloadPrefix(), Config.getImportExtension()));

		tcd.mapUpdates(recno, recno++, p, true);

		d.resetCounters();
		tcd.commit();
		d.resetConnections();

		//The delete option was removed.
		//Config.setDelete(true);

		runScalabilityInsDel(tcd, d, p, 1, 1);
//		runScalabilityInsDel(tcd, d, 10, 10);
//		runScalabilityInsDel(tcd, d, 100, 100);
//		runScalabilityInsDel(tcd, d, 500, 500);
//		runScalabilityInsDel(tcd, d, 1000, 1000);

		tcd.clean();

	}

	public static void runAllnoMigrate(SqlEngine tcd, SqlDb d, Peer p) throws Exception {	

//		for(int k = 100; k <= 1000; k*=10){
		int k = 2000;
//		for(int l = k/10; l <= k/3; l+=k/10){
//		for(int l = k/10; l <= k/2; l+=k/5){
		for(int l = 7*k/10; l < k; l+=k/5){
			Config.setWorkloadPrefix(Config.getTestSchemaName() + "-"+k+"i"+l+"d");

			System.out.println("================");
			System.out.println("SCHEMA: " + Config.getWorkloadPrefix());
			System.out.println("#INSERTIONS: " + k);
			System.out.println("#DELETIONS: " + l);
			System.out.println("================");

			System.out.println("EXP: NON-INCREMENTAL");
			SqlEngine.insTimes = new ArrayList<Long>();
			SqlEngine.delTimes = new ArrayList<Long>();
			runNonIncremental(tcd, p, 3);
			calcAvgTimes();

			System.out.println("EXP: INCREMENTAL");
			SqlEngine.insTimes = new ArrayList<Long>();
			SqlEngine.delTimes = new ArrayList<Long>();
			runIncremental(tcd, p, 3);
			calcAvgTimes();


		}
//		}

		System.out.println("================");
		System.out.println("DRED EXPERIMENT");
		System.out.println("================");
		
		tcd.clean();
		Config.setStratified(false);

		createDatabase(Config.getTestSchemaName(), d);
		tcd.migrate();

//		for(int l = k/10; l <= k/2; l+=k/5){
		for(int l = k/10; l <= k/2; l+=k/5){
		
			Config.setWorkloadPrefix(Config.getTestSchemaName() + "-"+k+"i"+l+"d");

			
			System.out.println("================");
			System.out.println("SCHEMA: " + Config.getWorkloadPrefix());
			System.out.println("#INSERTIONS: " + k);
			System.out.println("#DELETIONS: " + l);
			System.out.println("================");

			System.out.println("EXP: DRed");

			SqlEngine.insTimes = new ArrayList<Long>();
			SqlEngine.delTimes = new ArrayList<Long>();
			runIncremental(tcd, p, 3);
			calcAvgTimes();
		}

		tcd.clean();
	}
	
	public static void runAllnoMigrateInsOnly(SqlEngine tcd, Peer p) throws Exception {	

		int k = 1000;
		int l = 100;
		Config.setWorkloadPrefix(Config.getTestSchemaName() + "-"+k+"i"+l+"d");

		System.out.println("================");
		System.out.println("SCHEMA: " + Config.getWorkloadPrefix());
		System.out.println("#INSERTIONS: " + k);
		System.out.println("#DELETIONS: " + l);
		System.out.println("================");

		// The delete option was removed
		//Config.setDelete(false);
		System.out.println("EXP: INSERTIONS ONLY");
		SqlEngine.insTimes = new ArrayList<Long>();
		SqlEngine.delTimes = new ArrayList<Long>();
		runIncremental(tcd, p, 5);
		calcAvgTimes();

		tcd.clean();
	}
	
	public static void runOnce(SqlEngine tcd, Peer p) throws Exception {
		System.out.println("================");
		System.out.println("SCHEMA: " + Config.getWorkloadPrefix());
		System.out.println("================");

		SqlEngine.insTimes = new ArrayList<Long>();
		SqlEngine.delTimes = new ArrayList<Long>();
		runNonIncremental(tcd, p, 1);

		SqlEngine.insTimes = new ArrayList<Long>();
		SqlEngine.delTimes = new ArrayList<Long>();
		runIncremental(tcd, p, 1);
		calcAvgTimes();

		tcd.clean();
	}

	public static void runScalability1(SqlEngine tcd, SqlDb d, Peer p) throws Exception {
		Config.setWorkloadPrefix(Config.getTestSchemaName() + "-10000i0d");
		// The insert and delete options were removed.
		//Config.setInsert(true);
		//Config.setDelete(false);
		Config.setNonIncremental(false);
		
		tcd.importUpdates(new FileDb(Config.getWorkloadPrefix(), Config.getImportExtension()));
	
		tcd.mapUpdates(recno, ++recno, p, true);
		
		d.resetCounters();
		tcd.commit();
		d.resetConnections();		
	}

	public static void runScalability2(SqlEngine tcd, SqlDb d, Peer p) throws Exception {		
		
		runScalabilityInsDel(tcd, d, p, 100, 100);
		runScalabilityInsDel(tcd, d, p, 500, 500);
		runScalabilityInsDel(tcd, d, p, 1000, 1000);
		
		tcd.clean();
		
	}

}


