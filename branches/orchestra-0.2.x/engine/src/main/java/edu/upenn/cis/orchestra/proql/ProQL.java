package edu.upenn.cis.orchestra.proql;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.DatalogViewUnfolder;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.OuterJoinUnfolder;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;

public class ProQL {
	/**
	 * Tries to execute the query in the current text area
	 * 
	 * @throws Exception
	 */
	public static List<Tuple> runProvenanceQuery(String q, boolean printResults, boolean BFS, OrchestraSystem sys) throws Exception {
		//		String queryString = q.replace('\n', ' ');
		String queryString = q;
		List<Tuple> queryResults = new ArrayList<Tuple>();

		Calendar start = Calendar.getInstance();
		String evalExp = "";
		String pathExp = queryString;
		int eval = queryString.indexOf("EVALUATE");

		if(eval != -1){
			pathExp = queryString.substring(0, eval-1);
			evalExp = queryString.substring(eval);
			Config.setValueProvenance(true);
		}else{
			Config.setValueProvenance(false);
		}

		StringReader sr = new StringReader(pathExp);

		/*
		List<Tuple> results = new ArrayList<Tuple>();

		// Try to run it as non-recursive: more efficient
		try {
			results = _system.runUnfoldedQuery(new BufferedReader(sr));

		// If the program had recursion, we need to run it differently
		} catch (RecursionException re) {
			sr = new StringReader(_query.getText());
			results = _system.runMaterializedQuery(new BufferedReader(sr));
		}

		_data.clear();

//		// TODO:  get the results!!!
		for (Tuple tuple : results) {
			//_results.append(tuple.toString() + "\n");
			_data.addElement(tuple);
		}*/

		/** Some test-case queries to try:
		 * 
		 * For BIOSIMPLEZ:
		 * [PLASMODB.DOMAIN_REF $x] <- [INTERPRO.ENTRY2METH $z]
		 * [PLASMODB.REFSEQ $A] <- []
		 * [PLASMODB.REFSEQ $A] *- []
		 * 
		 * For JOIN3:
		 * [V1] *- []
		 * [V1] <- []
		 */
		Calendar before = Calendar.getInstance();

		Pattern queryPattern = QueryParser.getPatternFor(sr);

		System.out.println(queryPattern.toString());

		String semiringName = QueryParser.getAnnotationType(evalExp);
		String assgnExpr = QueryParser.getAssignmentExpression(evalExp);

		SchemaGraph g = new SchemaGraph(sys);
		System.out.println("Schema graph: " + g.toString());

		Set<SchemaSubgraph> results = MatchPatterns.getSubgraphs(g, queryPattern);

		Debug.println("Query results:"); 
		for (SchemaSubgraph sg : results) {
			Debug.println(sg.toString());

			Debug.println("Original program:");
			Calendar qsbefore = Calendar.getInstance();
			List<Rule> rules = sg.toQuerySet();
			Calendar qsafter = Calendar.getInstance();
			long qstime = qsafter.getTimeInMillis() - qsbefore.getTimeInMillis();
			System.out.println("EXP: NET TO QUERY SET TIME: " + qstime + " msec");


			Map<Atom,ProvenanceNode> prov = new HashMap<Atom,ProvenanceNode>();
			Set<String> provenanceRelations = new HashSet<String>();

			// Compute the set of provenance relations -- the unfolder needs to know about them
			for (RelationContext r : sys.getMappingEngine().getState().getMappingRelations())
				provenanceRelations.add(r.toString());

			// Assume the last rule is the distinguished variable - greg: NO, this is wrong and gives different results for different runs!!!
			//			List<Rule> program = DatalogViewUnfolder.unfoldQuery(rules, rules.get(0).getHead().getRelationContext().toString(),
			//			Need to add assignment here as well
			List<Rule> program = DatalogViewUnfolder.unfoldQuery(rules, sg.getRootNode().getName(),
					prov, provenanceRelations, semiringName, assgnExpr, BFS);

			System.out.println("\nUnfolded program has " + program.size() + " rules:");
			for (Rule r : program){
				Debug.println(r.toString());
				r.setOnlyKeyAndNulls();
			}
			
			System.out.println("PROV: " + prov.size());
			SqlEngine engine = (SqlEngine)sys.getMappingEngine();
			//			List<Rule> programWithASRs = OuterJoinUnfolder.unfoldOuterJoins(program, 
			List<Rule> programWithInnerASRs = OuterJoinUnfolder.unfoldOuterJoins(program,
					engine.getState().getInnerJoinRelations(), 
					AtomType.NONE, AtomType.NONE,
					engine.getMappingDb().getBuiltInSchemas());
			System.out.println("\nUnfolded program with Inner ASRs (" + programWithInnerASRs.size() + "):");
			for (Rule r : programWithInnerASRs){
				r.setDistinct(false);
				Debug.println(r.toString());
			}
			
			List<Rule> programWithRealASRs = OuterJoinUnfolder.unfoldOuterJoins(programWithInnerASRs,
					engine.getState().getRealOuterJoinRelations(), 
					AtomType.NONE, AtomType.NONE,
					engine.getMappingDb().getBuiltInSchemas());
			System.out.println("\nUnfolded program with Real OJ ASRs (" + programWithRealASRs.size() + "):");
			for (Rule r : programWithRealASRs){
				r.setDistinct(false);
				Debug.println(r.toString());
			}

			List<Rule> programWithAllASRs = OuterJoinUnfolder.unfoldOuterJoins(programWithRealASRs,
					engine.getState().getSimulatedOuterJoinRelations(), 
					AtomType.NONE, AtomType.NONE,
					engine.getMappingDb().getBuiltInSchemas());
			System.out.println("\nUnfolded program with All ASRs (" + programWithAllASRs.size() + "):");
			for (Rule r : programWithAllASRs){
				r.setDistinct(false);
				Debug.println(r.toString());
			}
			
			Calendar after = Calendar.getInstance();
			long time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("PROQL EXP: TOTAL QUERY UNFOLDING TIME : " + time + " msec");
			prov.clear();
			
			if(engine.getState().getRealOuterJoinRelations().size() > 0 ||
			   engine.getState().getSimulatedOuterJoinRelations().size() > 0 || 
			   engine.getState().getInnerJoinRelations().size() > 0){
				queryResults.addAll(sys.runUnfoldedQuery(programWithAllASRs, true, semiringName, printResults, true));
			}else{
				queryResults.addAll(sys.runUnfoldedQuery(programWithAllASRs, true, semiringName, printResults, false));
			}
		}
		if(printResults)
			System.out.println("EXP: RESULT SIZE : " + queryResults.size());

		Calendar end = Calendar.getInstance();
		long totalTime = end.getTimeInMillis() - start.getTimeInMillis();
		System.out.println("PROQL EXP: TOTAL PROQL TIME: " + totalTime + " msec");

		System.gc();
		return queryResults;
	}
}
