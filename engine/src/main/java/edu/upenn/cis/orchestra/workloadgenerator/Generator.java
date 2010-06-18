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
package edu.upenn.cis.orchestra.workloadgenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.upenn.cis.orchestra.datamodel.Relation;

/**
 * Here's what happens in each iteration:
 * <ol>
 * <li>Add in new peers.</li>
 * <li>Delete peers, but not any added in step 1.</li>
 * <li>Add bypasses to the graph that steps 1 and 2 generated.</li>
 * <li>Delete bypasses from the graph that steps 1 and 2, but not 3, generated.</li>
 * </ol>
 * It may have been equivalent, and simpler, to do it like this:
 * <ol>
 * <li>Delete peers.</li>
 * <li>Add peers.</li>
 * <li>Delete bypasses.</li>
 * <li>Add bypasses.</li>
 * </ol>
 * 
 * @param args command line arguments. See <code>buildOptions(...)</code>.
 */
public class Generator {

	/**
	 * Parameters to use when running this<code>Generator</code>. Could be
	 * from command line, for example.
	 */
	private Map<String, Object> _params;

	private boolean _inout;

	/** Initialze with first instance of <code>Generator</code>. */
	private static Random _random = null;

	public Generator(Map<String, Object> params, boolean inout) {
		this(params, inout, 0, (Integer) params.get("schemas"), null);
		_journal = new GeneratorJournal();
	}

	private int _start;
	private int _end;
	private Generator _previousGeneration;

	private GeneratorJournal _journal;

	public Generator(Map<String, Object> params, boolean inout, int start,
			int end, Generator previousGeneration) {
		// sanity check the parameters
		check("peers", "schemas", ">=", params);
		check("peers", "fanout", ">", params);
		check("deletions", "insertions", "<=", params);

		_params = params;
		_inout = inout;
		if (null == _random) {
			_random = new Random((Integer) _params.get("seed"));
		}
		_start = start;
		_end = end;
		_previousGeneration = previousGeneration;
		if (null != _previousGeneration) {
			_journal = _previousGeneration._journal;
		}
	}

	public String[] suffixes() {
		if (1 == (Integer) _params.get("olivier") && _inout) {
			if(1 == (Integer) _params.get("noreject"))
				return new String[] { "", "_L" };
			else
				return new String[] { "", "_L", "_R" };
		} else if (1 == (Integer) _params.get("olivier")) {
			return new String[] { "" };
		} else if (_inout) {
			return new String[] { "", "_INS", "_DEL", "_L", "_L_INS", "_L_DEL",
			"_R" };
		} else {
			return new String[] { "", "_INS", "_DEL" };
		}
	}

	/**
	 * A list of lists of attribute names: logical schemas contain relations
	 * contain attribute names. So <code>_logicalSchema.get(i)</code> is a
	 * logical schema and <code>_logicalSchema.get(i).get(i)</code> is a
	 * relation and <code>_logicalSchema.get(i).get(i).get(i)</code> is an
	 * attribute.
	 */
	private List<List<List<String>>> _logicalSchemas = new ArrayList<List<List<String>>>();

	/**
	 * Get the generated schemas.
	 * 
	 * @return the generated schemas.
	 */
	public List<List<List<String>>> getLogicalSchemas() {
		return _logicalSchemas;
	}

	/**
	 * <code>_peers[i]</code> is an index into the schemas list. So each peer
	 * is a list of schemas, stored as <code>_peers.get(i)</code>-><code>_logicalSchemas</code>.
	 * (Though the current implementation only consists of a single schema per
	 * peer.) Peer #i is located at peers.get(i)
	 */
	private List<Integer> _peers = new ArrayList<Integer>();

	/**
	 * Return the peers that were generated.
	 * 
	 * @return the peers that were generated.
	 */
	public List<Integer> getPeers() {
		return _peers;
	}

	/**
	 * <code>_mappings[i]</code> is a triple i, j, X where i, j are peers and
	 * X is a list of attributes. So, it's a mapping from i(X)->j(X). When the
	 * mappings are serialized to the schema file, the missing attributes on the
	 * left turned into _'s and missing attributes on the right turned into -'s.
	 */
	private List<List<Object>> _mappings = new ArrayList<List<Object>>();

	/**
	 * Get the mappings that have been generated.
	 * 
	 * @return the mappings that have been generated.
	 */
	public List<List<Object>> getMappings() {
		return _mappings;
	}

	/**
	 * Keeps track of which generation we're on during evolution.
	 */
	private int _generation = -1;

	public void generate() {
		//		List<List<Integer>> cycles = new ArrayList<List<Integer>>();
		//		while (true) {
		//		generate(0);
		//		findSimpleCycles(cycles, getMappings());
		//		if (-1 == (Integer) _params.get("mincycles")
		//		|| cycles.size() >= (Integer) _params.get("mincycles")) {
		//		if (-1 == (Integer) _params.get("maxcycles")
		//		|| cycles.size() <= (Integer)_params
		//		.get("maxcycles")) {
		//		break;
		//		}
		//		}
		//		}
		generate(0);
	}

	protected boolean acyclic() {
		return (Integer)_params.get("maxcycles") == 0;
	}

	public void topologyForDRedComparison(boolean fwd){
		int branchSize = _peers.size()/3;

		if(branchSize > 0){
			int i;

			//			First branch
			for(i = 0; i < branchSize-1; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			//			Second branch			
			for(i++; i < 2*branchSize; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			addMapping(branchSize-1, i, null, _mappings, fwd);

			for(; i < _peers.size()-1; i++){
				addMapping(i, i+1, null, _mappings, fwd);
				//           if (j % 3 == 0):
				//             addMapping(j, j+1, null, _mappings);
				//           else:
				//             addMapping(j+1, j, null, _mappings);
			}
		}
	}

	public void chainTopology(boolean fwd){
		for(int i = 0; i < _peers.size()-1; i++){
			addMapping(i, i+1, null, _mappings, fwd);
		}
	}

	public void veeTopology(boolean fwd){
		int branchSize = _peers.size()/2;
		int i;

		//	First branch
		for(i = 0; i < branchSize; i++){
			addMapping(i, i+1, null, _mappings, fwd);
		}

		// Second branch
		addMapping(0, i+1, null, _mappings, fwd);
		for(i++; i < _peers.size()-1; i++){
			addMapping(i, i+1, null, _mappings, fwd);
		}
	}

	public void diamondTopology(boolean fwd){
		int branchSize = _peers.size()/2;
		int i;

		//	First branch
		for(i = 0; i < branchSize; i++){
			addMapping(i, i+1, null, _mappings, fwd);
		}

		// Second branch
		addMapping(0, i+1, null, _mappings, fwd);
		for(i++; i < _peers.size()-1; i++){
			addMapping(i, i+1, null, _mappings, fwd);
		}
		addMapping(i, branchSize, null, _mappings, fwd);
	}

	public void chainVeeTopology(boolean fwd){
		int branchSize = _peers.size()/3;

		if(branchSize > 0){
			int i;

			//	Chain
			for(i = 0; i < _peers.size()-1-2*branchSize; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			int branchingNode = i;

			//	First branch			
			for(int j = 0; j < branchSize; j++, i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			//	Second branch
			addMapping(branchingNode, ++i, null, _mappings, fwd);

			for(; i < _peers.size()-1; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}
		}

	}

	public void multiBranchTopology(boolean fwd){
		int branchSize = _peers.size()/3;

		if(branchSize > 0){
			int i;

			//	Chain
			for(i = 0; i < _peers.size()-1-2*branchSize; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			int branchingNode = i;

			//	First branch			
			for(int j = 0; j < branchSize; j++, i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			//	Second branch
			addMapping(branchingNode/2, ++i, null, _mappings, fwd);

			for(; i < _peers.size()-1; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}
		}

	}
	
	public void doubleBranchTopology(boolean fwd){
		int branchSize = _peers.size()/4;

		if(branchSize > 0){
			int i;

			//	Chain
			for(i = 0; i < _peers.size()-1-2*branchSize; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			int branchingNode1 = i/3;
			int branchingNode2 = 2*i/3;

			addMapping(branchingNode1, ++i, null, _mappings, fwd);

			//	First branch			
			for(int j = 0; j < branchSize-1; j++, i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}

			//	Second branch
			addMapping(branchingNode2, ++i, null, _mappings, fwd);

			for(; i < _peers.size()-1; i++){
				addMapping(i, i+1, null, _mappings, fwd);
			}
		}

	}

	//	n-tree topology, with mappings from root to leaves
	public void naryTreeTopology(boolean fwd, int fanout){
		for(int i = 1; i <= fanout && i < _peers.size(); i++){
			addMapping(0, i, null, _mappings, fwd);
		}
		
		for(int i = fanout + 1; i < _peers.size(); i++){
			addMapping(i/fanout, i, null, _mappings, fwd);
		}
	}


	//	General, randomly generated topology, connecting all peers
	public void randomTopology(){

		// First add mappings to ensure all peers are
		// connected (connected in a undirected sense). The
		// resulting graph of mappings is always acyclic.

		// Store available targets for _peer[i] in allAvail[i].
		List<List<Integer>> allAvail = new ArrayList<List<Integer>>();

		// Fill in for peers for previous generations. To keep indexes lined up
		// for allAvail.
		for (int i = 0; i < _start; i++) {
			allAvail.add(new ArrayList<Integer>());
		}
		for (int i = _start; i < _peers.size(); i++) {
			// Available to peer i as a target.
			allAvail.add(new ArrayList<Integer>(_peers));

			// Don't want to map a peer to itself.
			allAvail.get(allAvail.size() - 1).remove((Integer) i);

			// Peers that are set to null are not available as targets.
			allAvail.get(allAvail.size() - 1).removeAll(
					Arrays.asList((Object) null));

			if (0 != i) {
				// Pick out a target peer for peer i.
				// Make the target of peer[i] be peer[0] < peer[i] so we'll
				// get a connected graph.
				List<Integer> candidatesForTarget = new ArrayList<Integer>();
				for (Integer peer : _peers) {
					if (null != peer && peer < i) {
						candidatesForTarget.add(peer);
					}
				}
				if (candidatesForTarget.size() == 0) {
					continue;
				}
				int j = candidatesForTarget.get(_random
						.nextInt(candidatesForTarget.size()));
				if (_random.nextBoolean()) {
					addMapping(i, j, allAvail.get(i), _mappings);
				} else {
					addMapping(j, i, allAvail.get(j), _mappings);
				}
				_journal.addMappingForPeer(i, _mappings.size() - 1, _mappings
						.get(_mappings.size() - 1));
			}
		}

		// Then add more mappings according to the fanout parameter.
		// These mappings may introduce cycles.
		for (int i = _start; i < _peers.size(); i++) {
			for (int j = 0; j < (Integer) _params.get("fanout"); j++) {
				if (0 != allAvail.get(i).size() && _random.nextBoolean()) {
					int k = _random.nextInt(allAvail.get(i).size());
					// generate a mapping from i to avail.get(k)
					// based on common attributes
					addMapping(i, allAvail.get(i).get(k), allAvail.get(i),
							_mappings);
					_journal.addMappingForPeer(i, _mappings.size() - 1,
							_mappings.get(_mappings.size() - 1));
				}
			}
		}
	}


	/**
	 * Generate an orchestra schema.
	 * 
	 * @param generation
	 *            the generation number.
	 */
	public void generate(int generation) {
		_generation = generation;
		// generate the random schemas
		for (int i = _start; i < _end; i++) {
			List<String> pi = new ArrayList<String>(Stats.getAtts());
			Collections.shuffle(pi, _random);
			List<List<String>> schema = new ArrayList<List<String>>();
			// fixed size relation
			int j = 0;
			schema.add(new ArrayList<String>());

			for (String att : pi) {
				if (_random.nextDouble() <= (Double) _params.get("coverage")) {
					if (schema.get(j).size() == (Integer) _params
							.get("relsize") - 1) {
						j++;
						schema.add(new ArrayList<String>());
					}
					schema.get(j).add(att);
				}
			}
			if((Boolean)_params.get("addValueAttr")){
				for(int k = 0; k < schema.size(); k++)
					schema.get(k).add(Relation.valueAttrName);
			}
			_logicalSchemas.add(schema);
		}

		for (int i = _start; i < _end; i++) {
			_peers.add(i);
			_journal.addPeer(_generation, i, _logicalSchemas.get(i - _start));
		}

		// THIS CODE IS LEFT OVER FROM ORIGINAL VERSION
		// assign peers to schemas
		// for (int i = end; i < (Integer) _params
		// .get("peers"); i++) {
		// int j = _random.nextInt(_logicalSchemas.size());
		// _peers.add(j);
		// }

		if (null != _previousGeneration) {
			merge(_previousGeneration);
		}

		// generate mappings among the peers
		// Param determines direction of the mappings (fwd: true, bwd: false)
		switch ((Integer) _params.get("topology")){
		case 0: 
			randomTopology();
			break;

		case 1: 
			topologyForDRedComparison(false);
			break;

		case 2:
			chainTopology(false);
			break;

		case 3:
			veeTopology(false);
			break;

		case 4:
			diamondTopology(false);
			break;

		case 5:
			chainVeeTopology(false);
			break;

		case 6:
			multiBranchTopology(false);
			break;

		case 7:
			//			naryTreeTopology(false, (Integer) _params.get("fanout"));
			naryTreeTopology(false, 2);
			break;

		case 8:
			naryTreeTopology(false, 3);
			break;

		case 9:
			naryTreeTopology(false, 4);
			break;

		case 10:
			naryTreeTopology(false, (Integer) _params.get("fanout"));
			break;

		case 11:
			doubleBranchTopology(false);
			break;
			
		default:	
			randomTopology();
		}
		//		for(Object o : _mappings){
		//			System.out.println(o.toString());
		//		}
	}

	private static int getSource(List<?> mapping) {
		return null == mapping ? -1 : (Integer) mapping.get(0);
	}

	private static int getTarget(List<?> mapping) {
		return null == mapping ? -1 : (Integer) mapping.get(1);
	}

	public void deletePeers(int numberOfPeersToDelete) {
		Set<Integer> peersToDelete = new HashSet<Integer>(numberOfPeersToDelete);
		List<Integer> candidatePeersToDelete = new LinkedList<Integer>();

		for (int i = 0; i < _previousGeneration.getPeers().size(); i++) {
			if (null != _previousGeneration.getPeers().get(i)) {
				candidatePeersToDelete.add(i);
			}
		}
		while (peersToDelete.size() < numberOfPeersToDelete
				&& candidatePeersToDelete.size() > 0) {
			peersToDelete.add(candidatePeersToDelete.remove(_random
					.nextInt(candidatePeersToDelete.size())));
		}
		for (Integer peerToDelete : peersToDelete) {
			// _logicalSchemas.set(_peers.get(peerToDelete.intValue()), null);
			_peers.set(peerToDelete.intValue(), null);
			for (int i = 0; i < _mappings.size(); i++) {
				List<Object> mapping = _mappings.get(i);
				if (peerToDelete.equals(getSource(mapping))
						|| peerToDelete.equals(getTarget(mapping))) {
					_mappings.set(i, null);
				}
			}
			_journal.deletePeer(_generation, peerToDelete);
		}
	}

	/**
	 * Map peer[i] to peer[j], mapping all common attributes to common
	 * attributes. Remove peer j (the target) from the available targets.
	 */
	private void addMapping(int i, int j, List<Integer> availTargets,
			List<List<Object>> mappings) {
		Set<String> s1 = new HashSet<String>(unnest(_logicalSchemas.get(i)));
		Set<String> s2 = new HashSet<String>(unnest(_logicalSchemas.get(j)));
		List<Object> mapping = new ArrayList<Object>();
		s1.retainAll(s2);
		mapping.add(i);
		mapping.add(j);
		mapping.add(new ArrayList<String>(s1));
		mappings.add(mapping);
		// Cast it to Integer - we want to remove the Integer from the list, not
		// the element located at position j, which happens if we leave j as an
		// int.
		if(availTargets != null)
			availTargets.remove((Integer) j);
	}

	private void addMapping(int i, int j, List<Integer> availTargets,
			List<List<Object>> mappings, boolean fwd) {
		if(fwd){
			addMapping(i, j, availTargets, mappings);
		}else{
			addMapping(j, i, availTargets, mappings);
		}
	}

	/**
	 * Flattens out <code>a</code>: all elements become top level elements.
	 * 
	 * @param a
	 *            to be flattened.
	 * @return a flattened version <code>a</code>.
	 */
	private List<String> unnest(List<List<String>> a) {
		List<String> b = new ArrayList<String>();
		for (List<String> thisA : a) {
			for (String thisThisA : thisA) {
				b.add(thisThisA);
			}
		}
		return b;
	}

	public void findSimpleCycles(List<List<Integer>> cycles,
			List<List<Object>> mappings) {
		// First, index the edges
		List<List<Integer>> edges = new ArrayList<List<Integer>>();

		for (int i = 0; i < _peers.size(); i++) {
			edges.add(new ArrayList<Integer>());
		}

		for (List<Object> thisMapping : mappings) {
			edges.get((Integer) thisMapping.get(0)).add(
					(Integer) thisMapping.get(1));
		}

		for (List<Integer> thisEdge : edges) {
			Collections.sort(thisEdge);
		}

		// Find simple cycles as follows:
		// - Handle the peers in order
		// - Find simple cycles where the smallest node in the cycle
		// is the peer
		cycles.clear();
		for (int i = 0; i < _peers.size(); i++) {
			Deque<List<Integer>> paths = new ArrayDeque<List<Integer>>();
			paths.push(new ArrayList<Integer>());
			paths.peek().add(i);
			while (0 != paths.size()) {
				List<Integer> path = paths.pop();
				for (Integer j : edges.get(path.get(path.size() - 1))) {
					if (j.equals(i)) {
						List<Integer> cycle = new ArrayList<Integer>();
						cycle.addAll(path);
						cycle.add(j);
						cycles.add(cycle);
					} else if (j > i && !path.contains(j)) {
						List<Integer> newPath = new ArrayList<Integer>();
						newPath.addAll(path);
						newPath.add(j);
						paths.push(newPath);
					}
				}
			}
		}
	}

	public void create(Generator generator) {
		Writer writer = null;
		if (null == _params.get("filename")) {
			writer = new PrintWriter(System.out);
		} else {
			try {
				writer = new FileWriter((String) _params.get("filename")
						+ ".create");
				// + ".create.new");
			} catch (IOException e) {
				throw new IllegalStateException(
						"Unable to open SQL create file.", e);
			}
		}
		try {
			writer.write(header());
			for (int i = 0; i < _journal.getPeers().size(); i++) {
				int j = _journal.getPeers().get(i);
				for (int k = 0; k < _journal.getLogicalSchemas().get(j).size(); k++) {
					for (String suffix : suffixes()) {
						writer.write("CREATE TABLE "
								+ WorkloadGeneratorUtils.relname(i, j, k)
								+ suffix + " (\n");
						writer.write("    KID INTEGER NOT NULL, \n");
						for (Iterator<String> itr = _journal
								.getLogicalSchemas().get(j).get(k).iterator(); itr
								.hasNext();) {
							writer.write("    " + realtyped(itr.next()));
							if (itr.hasNext()) {
								writer.write(", \n");
							}
						}
						writer.write("\n) " + noLoggingString() + ";\n");
					}
				}
			}
			writer.write(footer());
			if (1 == (Integer) _params.get("oracle")) {
				writer.write("EXIT\n");
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw new IllegalStateException(
					"Problem writing to SQL create file", e);
		}
	}

	private String header() {
		StringBuilder sb = new StringBuilder("-- "
				+ WorkloadGeneratorUtils.stamp() + _params + "\n");
		if (0 == (Integer) _params.get("oracle")) {
			sb.append("CONNECT TO " + _params.get("dbalias") + " USER "
					+ _params.get("username") + " USING "
					+ "'" + _params.get("password") + "'" + ";\n");
		}
		return sb.toString();
	}

	private String footer() {
		if (0 == (Integer) _params.get("oracle")) {
			return "COMMIT WORK;\nDISCONNECT ALL;\n";
		}
		return "";
	}

	private String realtyped(String a) {
		if (1 == (Integer) _params.get("integers") || ((Boolean)_params.get("addValueAttr") && a.equals(Relation.valueAttrName))) {
			return a + " INTEGER";
		} else {
			return a
			+ " VARCHAR("
			+ Math.min((Integer) Stats.getSize(a), (Integer) _params
					.get("cutoff")) + ")";
		}
	}

	private String noLoggingString() {
		if (1 == (Integer) _params.get("oracle")) {
			return "NOLOGGING";
		} else {
			return "NOT LOGGED INITIALLY";
		}
	}

	public void fill() {
		try {
			Writer insertWriter = null, deleteWriter = null;
			if (null == _params.get("filename")) {
				insertWriter = new PrintWriter(System.out);
				deleteWriter = new PrintWriter(System.out);
			} else {
				insertWriter = new FileWriter((String) _params.get("filename")
						+ ".insert");
				// + ".insert.new");
				deleteWriter = new FileWriter((String) _params.get("filename")
						+ ".delete");
				// + ".delete.new");
			}

			insertWriter.write(fillheader());
			deleteWriter.write(fillheader());

			BufferedReader f = null;
			f = new BufferedReader(new FileReader("../uniprot_sprot.dat"));
			
			// Skip the first _params.get("skip") entries
			Integer key = (Integer) _params.get("skip");
			for (int i = 0; i < (Integer) _params.get("skip"); i++) {
				Map<String, String> entry = nextentry(f);
				if (null == entry) {
					f.close();
					f = new BufferedReader(new FileReader(
					"../uniprot_sprot.dat"));
					entry = nextentry(f);
					assert null != entry;
				}
			}

			int redundancy = ((Integer) _params.get("redundancy"));
			int numDisjointPeers = _journal.getPeers().size()/redundancy;
			int remainingPeers = _journal.getPeers().size()%redundancy;

			// populate the peers - put the same data to all peers with equal % redundancy
			for (int i = 0; i < numDisjointPeers; i++) {
				List<List<Writer>> bulkIns = new ArrayList<List<Writer>>();
				List<List<Writer>> bulkDel = new ArrayList<List<Writer>>();

				for(int j = 0; j < redundancy; j++){
					if((j*numDisjointPeers + i < _journal.getPeers().size()) &&
							peerHasLocalData(j*numDisjointPeers + i, (Integer) _params.get("topology"), 
									(Integer) _params.get("modlocal"), (Integer) _params.get("peers"), (Integer) _params.get("fanout"))
					){
						bulkIns.add(bulkOpen(insertWriter, j*numDisjointPeers + i, insSuffix()));
						bulkDel.add(bulkOpen(deleteWriter, j*numDisjointPeers + i, delSuffix()));
					}
				}

				// populate each peer with datasize entries
				int updates = 0;
				for (int d = 0; d < (Integer) _params.get("insertions"); d++) {
					// grab the next entry from swissprot
					Map<String, String> entry = nextentry(f);
					if (null == entry) {
						f.close();
						f = new BufferedReader(new FileReader(
						"../uniprot_sprot.dat"));
						entry = nextentry(f);
						assert null != entry;
					}
					assert 0 < entry.keySet().size();
					int jj = 0;
					for(int j = 0; j < redundancy; j++){
						if((j*numDisjointPeers + i < _journal.getPeers().size()) &&
								peerHasLocalData(j*numDisjointPeers + i, (Integer) _params.get("topology"), 
										(Integer) _params.get("modlocal"), (Integer) _params.get("peers"), (Integer) _params.get("fanout")) 
						) {

							writeentry(bulkIns.get(jj), entry, insSuffix(), j*numDisjointPeers + i, key, _journal
									.getLogicalSchemas(), _journal.getPeers());
							if (updates < (Integer) _params.get("deletions")) {
								writeentry(bulkDel.get(jj), entry, delSuffix(), j*numDisjointPeers + i, key,
										_journal.getLogicalSchemas(), _journal
										.getPeers());
							}
							jj++;
						}
					}
					updates++;
					key++;
				}

				for(int j = 0; j < bulkIns.size(); j++){
					bulkClose(bulkIns.get(j));
					bulkClose(bulkDel.get(j));
				}
			}

			// populate the remaining peers
			for (int k = 0; k < remainingPeers; k++) {
				if(peerHasLocalData(_peers.size()-k-1, (Integer) _params.get("topology"), 
						(Integer) _params.get("modlocal"), (Integer) _params.get("peers"), (Integer) _params.get("fanout"))){
					List<Writer> bulkIns = bulkOpen(insertWriter, _peers.size()-k-1, insSuffix());
					List<Writer> bulkDel = bulkOpen(deleteWriter, _peers.size()-k-1, delSuffix());

					// populate each peer with datasize entries
					int updates = 0;
					for (int d = 0; d < (Integer) _params.get("insertions"); d++) {
						// grab the next entry from swissprot
						Map<String, String> entry = nextentry(f);
						if (null == entry) {
							f.close();
							f = new BufferedReader(new FileReader(
							"../uniprot_sprot.dat"));
							entry = nextentry(f);
							assert null != entry;
						}
						assert 0 < entry.keySet().size();

						writeentry(bulkIns, entry, insSuffix(), _peers.size()-k-1, key, _journal
								.getLogicalSchemas(), _journal.getPeers());
						if (updates < (Integer) _params.get("deletions")) {
							writeentry(bulkDel, entry, delSuffix(), _peers.size()-k-1, key,
									_journal.getLogicalSchemas(), _journal
									.getPeers());
						}
						updates++;
						key++;
					}

					bulkClose(bulkIns);
					bulkClose(bulkDel);
				}
			}

			f.close();
			insertWriter.write(footer());
			deleteWriter.write(footer());

			insertWriter.flush();
			insertWriter.close();
			deleteWriter.flush();
			deleteWriter.close();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public static boolean peerHasLocalData(int peer, int topology, int modlocal, int peers, int fanout){

		switch (topology) {
		case 0: return ((peer + 1) % modlocal == 0);

		case 2: return ((peer == peers-1) || ((peer + 1) % modlocal == 0));

		case 3: return ((peer == peers-1) || (peer == peers/2) || ((modlocal == peers/2) ? ((peer == modlocal/2) || (peer == 3*modlocal/2)) : false));

		case 4: return ((peer == peers/2) || ((modlocal == peers/2) ? ((peer == modlocal/2) || (peer == 3*modlocal/2)) : false));

		case 5: return ((peer == 2*(peers/3)) || (peer == peers-1) || ( (modlocal == peers/2) ? (peer == (2*modlocal)/3) : false) );

		case 6: return ((peer == 2*(peers/3)) || (peer == peers-1)  || ( (modlocal == peers/2) ? (peer == ((2*modlocal)/3)/2) : false) );

		case 7: return ((2*(peer+1) > peers) || ( (modlocal == peers/2) ? (2*2*(peer+1) <= peers) && (4*2*(peer+1) > peers) : false) );

		case 8: return ((3*(peer+1) > peers) || ( (modlocal == peers/2) ? (3*2*(peer+1) <= peers) && (9*2*(peer+1) > peers) : false) );

		case 9: return ((4*(peer+1) > peers) || ( (modlocal == peers/2) ? (4*2*(peer+1) <= peers) && (16*2*(peer+1) > peers) : false) );

		case 10: return ((fanout*(peer+1) > peers) || ( (modlocal == peers/2) ? (fanout*2*(peer+1) <= peers) && (fanout*fanout*2*(peer+1) > peers) : false) );

		case 11: int branchSize = peers/4; 
		         int chain = peers-1-2*branchSize;
			return ( (peer == chain) || (peer == (chain + branchSize)) || (peer == peers-1)  || ( ( (modlocal == peers/2) || (modlocal == chain) )? (peer == chain/2 || ( peer == chain + branchSize/2) || (peer == peers-1-branchSize/2)) : false) );
		
		default: return ((peer + 1) % modlocal == 0);

		}
	}

	private String fillheader() {
		if (1 == (Integer) _params.get("oracle")) {
			return "#!/usr/bin/sh\n#\n# " + WorkloadGeneratorUtils.stamp()
			+ " " + _params + "\n#\n";
		} else {
			return header();
		}
	}

	/**
	 * Parses the next entry from the swissprot flat file.
	 * 
	 * @param r
	 *            from which we're reading.
	 */
	private Map<String, String> nextentry(BufferedReader r) throws IOException {
		Map<String, String> entry = new HashMap<String, String>();
		while (true) {
			String line = r.readLine();
			if(line == null)
				return null;
			
			if ("".equals(line) || line.startsWith("//")) {
				break;
			}
			String key = line.substring(0, 2);
			String data = line.substring(5, line.length()).trim();
			if ("  ".equals(key)) {
				key = "SD"; // sequence data
			}
			if (null != entry.get(key)) {
				data = entry.get(key) + " " + data;
			}
			int maxlen = Math.min((Integer) _params.get("cutoff"), Stats
					.getSize(key));
			entry.put(key, data.substring(0, Math.min(maxlen, data.length())));
		}
		if (0 == entry.size()) {
			return null;
		}
		return entry;
	}

	private List<Writer> bulkOpen(Writer writer, int i, String suffix)
	throws IOException {
		if (1 == (Integer) _params.get("oracle")) {
			return bulkOpenOracle(writer, i, suffix);
		} else {
			return bulkOpenDB2(writer, i, suffix);
		}
	}

	private List<Writer> bulkOpenDB2(Writer writer, int i, String suffix)
	throws IOException {
		List<Writer> a = new ArrayList<Writer>();
		// if (null == _peers.get(i)) {
		// return a;
		// }
		int j = _journal.getPeers().get(i);
		for (int k = 0; k < _logicalSchemas.get(j).size(); k++) {
			String name = WorkloadGeneratorUtils.relname(i, j, k) + suffix;
			if (null == _params.get("filename")) {
				a.add(new PrintWriter(System.out));
			} else {
				//				gregkar - create workloads for import-noscript vs. -bulk
				//				a.add(new FileWriter((String) name + ".txt"));
				a.add(new FileWriter((String) _params.get("filename") + "."
						+ name));
			}
			String fname = null;
			if (null == _params.get("filename")) {
				fname = "";
			} else {
				fname = (String) _params.get("filename") + ".";
			}
			String tablename = WorkloadGeneratorUtils.relname(i, j, k) + suffix;
			StringBuilder attsBuilder = new StringBuilder("KID, ");
			for (Iterator<String> itr = _logicalSchemas.get(j).get(k)
					.iterator(); itr.hasNext();) {
				attsBuilder.append(itr.next());
				if (itr.hasNext()) {
					attsBuilder.append(", ");
				}
			}
			writer.write("IMPORT FROM " + fname + tablename);
			List<String> ran = new ArrayList<String>();
			for (int l = 1; l < _logicalSchemas.get(j).get(k).size() + 2; l++) {
				ran.add(String.valueOf(l));
			}
			writer.write(" OF DEL MODIFIED BY COLDEL| METHOD P (");
			for (Iterator<String> ranItr = ran.iterator(); ranItr.hasNext();) {
				writer.write(ranItr.next());
				if (ranItr.hasNext()) {
					writer.write(", ");
				}
			}
			writer.write(") MESSAGES NUL INSERT INTO " + tablename + "("
					+ attsBuilder + ");\n");
			writer.write("RUNSTATS ON TABLE " + _params.get("username") + "."
					+ tablename + " ON ALL COLUMNS ALLOW WRITE ACCESS;\n");
		}
		return a;
	}

	private List<Writer> bulkOpenOracle(Writer writer, int i, String suffix)
	throws IOException {
		List<Writer> a = new ArrayList<Writer>();
		int j = _journal.getPeers().get(i);
		for (int k = 0; k < _logicalSchemas.get(j).size(); k++) {
			String name = WorkloadGeneratorUtils.relname(i, j, k) + suffix;
			if (null == _params.get("filename")) {
				a.add(new PrintWriter(System.out));
			} else {
				a.add(new FileWriter((String) _params.get("filename") + "."
						+ name));
			}
			String fname = null;
			if (null == _params.get("filename")) {
				fname = "";
			} else {
				fname = _params.get("filename") + ".";
			}
			String tablename = WorkloadGeneratorUtils.relname(i, j, k) + suffix;
			writer.write("sqlldr " + _params.get("username") + "/"
					+ _params.get("password") + "@" + _params.get("dbalias")
					+ " CONTROL=" + fname + tablename + ".ctl" + " LOG="
					+ fname + tablename + ".log\n");
			List<String> typed = new ArrayList<String>();
			if (1 == (Integer) _params.get("integers")) {
				for (String att : _logicalSchemas.get(j).get(k)) {
					typed.add(att + " INTEGER");
				}
			} else {
				for (String att : _logicalSchemas.get(j).get(k)) {
					typed.add(att + " CHAR(" + _params.get("cutoff") + ")");
				}
			}
			StringBuilder attsBuilder = new StringBuilder("KID, ");
			for (Iterator<String> itr = typed.iterator(); itr.hasNext();) {
				attsBuilder.append(itr.next());
				if (itr.hasNext()) {
					attsBuilder.append(", ");
				}
			}
			Writer ctlfile = new FileWriter((String) _params.get("filename")
					+ "." + tablename + ".ctl");
			ctlfile.write("Load DATA\n");
			ctlfile.write("INFILE '" + fname + tablename + "'\n");
			ctlfile.write("BAFILE '" + fname + tablename + ".bad'\n");
			ctlfile.write("INTO TABLE " + tablename + "\n");
			ctlfile.write("FIELDS TERMINATED BY '|'\n");
			ctlfile.write("TRAILING NULLCOLS\n");
			ctlfile.write("(" + attsBuilder + ")\n\n");
			ctlfile.flush();
			ctlfile.close();
		}
		return a;
	}

	private String insSuffix() {
		if (_inout) {
			return "_L_INS";
		} else {
			return "_INS";
		}
	}

	private String delSuffix() {
		if (_inout) {
			return "_L_DEL";
		} else {
			return "_DEL";
		}
	}

	private String escape(String s) {
		return s.replace('|', ':').replace('"', '_');
	}

	private void writeentry(List<Writer> writers, Map<String, String> entry,
			String sufix, int i, int key, List<List<List<String>>> schemas,
			List<Integer> peers) throws IOException {
		// populate each relation in peer i
		if (null == peers.get(i)) {
			return;
		}
		int j = peers.get(i);
		for (int k = 0; k < schemas.get(j).size(); k++) {
			List<String> vals = new ArrayList<String>(Arrays.asList(String
					.valueOf(key)));
			for (String att : schemas.get(j).get(k)) {
				if (null == entry.get(att)) {
					if((Boolean)_params.get("addValueAttr") && Relation.valueAttrName.equals(att))
						vals.add("1");
					else
						vals.add("null");
				} else {
					if (1 == (Integer) _params.get("integers")) {
						vals.add(String.valueOf(Math.abs(entry.get(att)
								.hashCode())));
					} else {
						vals.add(escape(entry.get(att)));
					}
				}
			}
			for (Iterator<String> itr = vals.iterator(); itr.hasNext();) {
				writers.get(k).write(itr.next());
				if (itr.hasNext()) {
					writers.get(k).write("|");
				}
			}
			writers.get(k).write('\n');
		}
	}

	private void bulkClose(List<Writer> writers) throws IOException {
		if (null == _params.get("filename")) {
			// nothing to close
		} else {
			for (Writer writer : writers) {
				writer.flush();
				writer.close();
			}
		}
		writers.clear();
	}

	public void destroy() {
		try {
			Writer writer = null;
			if (null == _params.get("filename")) {
				writer = new PrintWriter(System.out);
			} else {
				writer = new FileWriter((String) _params.get("filename")
						+ ".destroy");
				// + ".destroy.new");
			}
			writer.write(header());
			for (int i = 0; i < _journal.getPeers().size(); i++) {
				for (String suffix : suffixes()) {
					int j = _journal.getPeers().get(i);
					for (int k = 0; k < _journal.getLogicalSchemas().get(j).size(); k++) {
						writer.write("DROP TABLE "
								+ WorkloadGeneratorUtils.relname(i, j, k)
								+ suffix + ";\n");
					}
				}
			}
			writer.write(footer());
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw new IllegalStateException(
					"Problem working with destroy file.", e);
		}
	}

	/**
	 * Find the head/tail pairs of all paths in the graph that have lengths
	 * greater than 1. For example if 1->5->8 and 3->17->2 are paths , return
	 * {(1, 8), (3, 2)}.
	 * 
	 * @return see description.
	 */
	private Set<List<Integer>> findCandidatesForBypasses() {
		List<List<Integer>> paths = new ArrayList<List<Integer>>();
		for (List<Object> mapping : _mappings) {
			if (!_peers.contains(getSource(mapping))
					|| !_peers.contains(getTarget(mapping))) {
				// We're only interested if the mapping is still relevant.
				continue;
			}
			paths.add(new ArrayList<Integer>(Arrays.asList(getSource(mapping),
					getTarget(mapping))));
		}
		Set<List<Integer>> sourceAndTargets = new HashSet<List<Integer>>();
		while (true) {
			List<List<Integer>> newPaths = new ArrayList<List<Integer>>();
			for (int i = 0; i < paths.size(); i++) {
				List<Integer> path = paths.get(i);
				for (List<Object> mapping : _mappings) {
					if (!_peers.contains(getSource(mapping))
							|| !_peers.contains(getTarget(mapping))) {
						continue;
					}
					if (null == mapping) {
						continue;
					}
					int candSource = (Integer) mapping.get(0);
					int candTarget = (Integer) mapping.get(1);
					if (path.get(path.size() - 1).equals(candSource)
							&& !path.contains(candTarget)) {
						newPaths.add(new ArrayList<Integer>(path));
						newPaths.get(newPaths.size() - 1).add(candTarget);
					}
				}
				// Get rid of the path after we're done examining it.
				// Only want paths longer than 1 (so greater than 2 nodes).
				if (path.size() > 2) {
					sourceAndTargets.add(Arrays.asList(path.get(0), path
							.get(path.size() - 1)));
				}
				paths.set(i, null);
			}
			if (0 == newPaths.size()) {
				break;
			}
			paths = newPaths;
		}
		return sourceAndTargets;
	}

	private Set<List<Integer>> findCandidateAddBypasses(Set<List<Integer>> paths) {
		Set<List<Integer>> candidateAddBypasses = new HashSet<List<Integer>>();
		for (List<Integer> path : paths) {
			for (List<Object> mapping : _mappings) {
				if (getSource(mapping) == path.get(0)
						&& (getTarget(mapping) == path.get(path.size() - 1))) {
					// It's already a mapping, so it's not a candidate
				} else {
					candidateAddBypasses.add(Arrays.asList(path.get(0), path
							.get(path.size() - 1)));
				}
			}
		}
		return candidateAddBypasses;
	}

	private Set<List<Integer>> findCandidateDeleteBypasses(
			Set<List<Integer>> paths) {
		Set<List<Integer>> candidateDeleteBypasses = new HashSet<List<Integer>>();
		for (List<Integer> path : paths) {
			for (List<Object> mapping : _mappings) {
				if (null == mapping) {
					continue;
				}
				if (((Integer) mapping.get(0)).equals(path.get(0))
						&& ((Integer) mapping.get(1)).equals(path.get(path
								.size() - 1))) {
					candidateDeleteBypasses.add(Arrays.asList(path.get(0), path
							.get(path.size() - 1)));

				}
			}
		}
		return candidateDeleteBypasses;
	}

	private Set<List<Integer>> chooseBypasses(
			Set<List<Integer>> candidateBypasses, int numOfBypasses) {
		Set<List<Integer>> bypasses = new HashSet<List<Integer>>();
		List<List<Integer>> candidateBypassesList = new LinkedList<List<Integer>>(
				candidateBypasses);
		while (bypasses.size() < numOfBypasses
				&& candidateBypassesList.size() > 0) {
			bypasses.add(candidateBypassesList.remove(_random
					.nextInt(candidateBypassesList.size())));
		}
		return bypasses;
	}

	private int deleteMapping(int source, int target,
			List<List<Object>> mappings) {
		for (int i = 0; i < mappings.size(); i++) {
			List<Object> mapping = mappings.get(i);
			if (source == getSource(mapping) && target == getTarget(mapping)) {
				mappings.set(i, null);
				return i;
			}
		}
		return -1;
	}

	private void addBypasses(Set<List<Integer>> addBypasses) {
		for (List<Integer> addBypass : addBypasses) {
			addMapping(getSource(addBypass), getTarget(addBypass),
					new LinkedList<Integer>(Arrays.asList(addBypass.get(1))),
					_mappings);
			_journal.addBypass(_generation, _mappings.size() - 1, _mappings
					.get(_mappings.size() - 1));
		}
	}

	private void deleteBypasses(Set<List<Integer>> deleteBypasses) {
		for (List<Integer> deleteBypass : deleteBypasses) {
			int deletedMapping = deleteMapping(deleteBypass.get(0),
					deleteBypass.get(1), _mappings);
			_journal.deleteBypass(_generation, deletedMapping);
		}
	}

	public void addAndDeleteBypasses(int numOfAddBypasses,
			int numOfDeleteBypasses) {
		Set<List<Integer>> candidatesForBypasses = findCandidatesForBypasses();

		// we need to get rid of any that have been deleted in this
		// <code>Generator</code>.
		// candidatesForBypasses.retainAll(getSourceAndTargetPeers(_mappings));
		Set<List<Integer>> addBypasses = chooseBypasses(
				findCandidateAddBypasses(candidatesForBypasses),
				numOfAddBypasses);
		Set<List<Integer>> deleteBypasses = chooseBypasses(
				findCandidateDeleteBypasses(candidatesForBypasses),
				numOfDeleteBypasses);
		addBypasses(addBypasses);
		deleteBypasses(deleteBypasses);
	}

	public void cycles(List<List<Integer>> cycles) {
		try {
			Writer writer = null;
			if (null == _params.get("filename")) {
				writer = new PrintWriter(System.out);
			} else {
				writer = new FileWriter((String) _params.get("filename")
						+ ".cycles");
				// + ".cycles.new");
			}
			writer.write("// " + WorkloadGeneratorUtils.stamp() + "\n");
			writer.write("// " + _params + "\n");
			writer.write("Simple cycle count: " + cycles.size() + "\n");
			writer.write("Simple cycles: " + cycles + "\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw new IllegalStateException(
					"Problem working with cycles file.", e);
		}
	}

	public void merge(Generator generator) {
		List<List<List<String>>> newLogicalSchemas = new ArrayList<List<List<String>>>(
				generator.getLogicalSchemas());
		newLogicalSchemas.addAll(_logicalSchemas);
		_logicalSchemas = newLogicalSchemas;

		List<Integer> newPeers = new ArrayList<Integer>(generator.getPeers());
		newPeers.addAll(_peers);
		_peers = newPeers;

		List<List<Object>> newMappings = new ArrayList<List<Object>>(generator
				.getMappings());
		newMappings.addAll(_mappings);
		_mappings = newMappings;
	}

	private static Map<String, Object> buildParams(CommandLine commandLine) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("skip", Integer.valueOf(commandLine.getOptionValue("skip",
		"0")));
		params.put("bidir", commandLine.hasOption("bidir") ? 1 : 0);
		params.put("cutoff", Integer.valueOf(commandLine.getOptionValue(
				"cutoff", "1024")));
		params.put("deletions", Integer.valueOf(commandLine.getOptionValue(
				"deletions", "1")));
		params.put("oracle", commandLine.hasOption("oracle") ? 1 : 0);
		params.put("filename", commandLine.getOptionValue("filename"));
		params.put("insertions", Integer.valueOf(commandLine.getOptionValue(
				"insertions", "1")));
		params.put("tukwila", commandLine.hasOption("tukwila") ? 1 : 0);
		params.put("maxcycles", Integer.valueOf(commandLine.getOptionValue(
				"maxcycles", "-1")));
		params.put("mincycles", Integer.valueOf(commandLine.getOptionValue(
				"mincycles", "-1")));
		params.put("relsize", Integer.valueOf(commandLine.getOptionValue(
				"relsize", "4")));
		params.put("fanout", Integer.valueOf(commandLine.getOptionValue(
				"fanout", "1")));
		params.put("olivier", commandLine.hasOption("olivier") ? 1 : 0);
		params.put("noreject", commandLine.hasOption("noreject") ? 1 : 0);
		params.put("nolocal", commandLine.hasOption("nolocal") ? 1 : 0);
		params.put("peers", Integer.valueOf(commandLine.getOptionValue("peers",
		"3")));
		params.put("seed", Integer.valueOf(commandLine.getOptionValue("seed",
		"0")));
		params.put("topology", Integer.valueOf(commandLine.getOptionValue("topology",
		"0")));
		params.put("redundancy", Integer.valueOf(commandLine.getOptionValue("redundancy",
		"1")));
		params.put("modlocal", Integer.valueOf(commandLine.getOptionValue("modlocal",
		"1")));
		params.put("schemas", Integer.valueOf(commandLine.getOptionValue(
				"schemas", params.get("peers").toString())));
		params.put("integers", commandLine.hasOption("integers") ? 1 : 0);
		params.put("updateAlias", commandLine.getOptionValue("updateAlias"));
		params.put("coverage", Double.valueOf(commandLine.getOptionValue(
				"coverage", "0.75")));
		params.put("dbalias", commandLine.getOptionValue("dbalias", "DEFEAT"));
		params.put("username", commandLine
				.getOptionValue("username", "DEFAULT_USER"));
		params.put("password", commandLine.getOptionValue("password",
		""));
		params.put("mappingsServer", commandLine.getOptionValue(
				"mappingsServer", "jdbc:db2://localhost:50000"));
		String mappingsServer = (String) params.get("mappingsServer");
		if (mappingsServer.endsWith("/")) {
			params.put("mappingsServer", mappingsServer.substring(0,
					mappingsServer.length() - 1));
		}
		params.put("addPeers", Integer.parseInt(commandLine.getOptionValue(
				"addPeers", "0")));
		params.put("deletePeers", Integer.parseInt(commandLine.getOptionValue(
				"deletePeers", "0")));
		params.put("addBypasses", Integer.parseInt(commandLine.getOptionValue(
				"addBypasses", "0")));
		params.put("deleteBypasses", Integer.parseInt(commandLine
				.getOptionValue("deleteBypasses", "0")));
		params.put("iterations", Integer.parseInt(commandLine.getOptionValue(
				"iterations", "0")));
		params.put("inout", commandLine.hasOption("inout") ? true : false);
		params.put("addValueAttr", commandLine.hasOption("addValueAttr") ? true : false);
		//		params.put("inout", true);
		return params;
	}

	@SuppressWarnings("static-access")
	private static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help").withDescription(
		"Display this usage message.").create('?'));
		options.addOption(OptionBuilder.withLongOpt("skip").withDescription(
		"Skip the first N Swissprot entries (default is 0)").hasArg()
		.create('a'));
		options.addOption(OptionBuilder.withLongOpt("bidir").withDescription(
		"Generate the bidirectional mappings").create('b'));
		options
		.addOption(OptionBuilder
				.withLongOpt("cutoff")
				.withDescription(
				"Set maximum VARCHAR size in generated schemas (default is 1024).")
				.hasArg().create('c'));
		options
		.addOption(OptionBuilder
				.withLongOpt("deletions")
				.withDescription(
						"Set deletion load as measured in number of Swissprot entries "
						+ "per peer to be d (default is 1). The deletion load may "
						+ "be no greater than the insertion load.")
						.hasArg().create('d'));
		options.addOption(OptionBuilder.withLongOpt("oracle").withDescription(
		"Output scripts for Oracle rather than DB2").create('e'));
		options
		.addOption(OptionBuilder
				.withLongOpt("filename")
				.withDescription(
						"Set filename prefix to use for output files. If "
						+ "unspecified, the program writes to standard output.")
						.hasArg().create('f'));
		options
		.addOption(OptionBuilder
				.withLongOpt("insertions")
				.withDescription(
				"Set insertion load as measured in number of Swissprot entries per peer to be inserted (default is 1).")
				.hasArg().create('i'));
		options.addOption(OptionBuilder.withArgName("tukwila").withDescription(
		"Use tukwila engine (default is DB2)").create('j'));
		options
		.addOption(OptionBuilder
				.withLongOpt("maxcycles")
				.withDescription(
				"Set maximum number of simple cycles (default is no maximum).")
				.hasArg().create('k'));
		options
		.addOption(OptionBuilder
				.withLongOpt("mincycles")
				.withDescription(
				"Set minimum number of simple cycles (default is no minimum).")
				.hasArg().create('l'));
		options
		.addOption(OptionBuilder
				.withLongOpt("relsize")
				.withDescription(
						"Set standard relation size -- the number of attributes -- in peer relations "
						+ "(default is 4). All but one relation in each peer will have exactly this "
						+ "many attributes (the other will have at most this many).")
						.hasArg().create('m'));
		options
		.addOption(OptionBuilder
				.withLongOpt("fanout")
				.withDescription(
				"Set maximum fanout from a peer in the graph of mappings (default is 1).")
				.hasArg().create('n'));
		options
		.addOption(OptionBuilder
				.withLongOpt("olivier")
				.withDescription(
				"Suppress CREATE TABLE commands for the _INS and _DEL relations")
				.create('o'));
		options
		.addOption(OptionBuilder
				.withLongOpt("noreject")
				.withDescription(
				"Suppress rejection tables (_R)")
				.create("nr"));
		options
		.addOption(OptionBuilder
				.withLongOpt("nolocal")
				.withDescription(
				"Suppress local tables (_L) in schema file")
				.create("nl"));
		options.addOption(OptionBuilder.withLongOpt("peers").withDescription(
		"Set number of peers (default is 3).").hasArg().create('p'));
		options.addOption(OptionBuilder.withLongOpt("seed").withDescription(
		"Set random number generator seed (default is 0).").hasArg()
		.create('r'));
		options
		.addOption(OptionBuilder
				.withLongOpt("schemas")
				.withDescription(
				"Set number of logical schemas (default is number of logical peers).")
				.hasArg().create('s'));
		options
		.addOption(OptionBuilder
				.withLongOpt("topology")
				.withDescription(
				"Select one of the special topologies for ProQL experiments (default is 0, for random topology)")
				.hasArg().create('t'));
		options
		.addOption(OptionBuilder
				.withLongOpt("redundancy")
				.withDescription(
				"Redundancy factor for source data, i.e., number of different peers the same data appear in (default is 1, for disjoint data at each peer)")
				.hasArg().create("rd"));
		options
		.addOption(OptionBuilder
				.withLongOpt("modlocal")
				.withDescription(
				"Peers for which (% this number) == 0 have local data in an _L relation")
				.hasArg().create("ml"));
		options
		.addOption(OptionBuilder
				.withLongOpt("integers")
				.withDescription(
				"Use integers intead of strings in workload, with values obtained by hashing input strings.")
				.create('u'));
		options
		.addOption(OptionBuilder
				.withLongOpt("updateAlias")
				.withDescription(
				"Set database-alias for secondary update store (default None)")
				.hasArg().create('w'));
		options
		.addOption(OptionBuilder
				.withLongOpt("coverage")
				.withDescription(
						"Set average fraction of full set of attributes contained by a peer "
						+ "(default is 0.75). Set to 1 to ensure all peers have all attributes.")
						.hasArg().create('v'));
		options.addOption(OptionBuilder.withLongOpt("dbalias").withDescription(
		"Set database-alias for CONNECT TO (default 'DEFEAT')")
		.hasArg().create('x'));
		options.addOption(OptionBuilder.withLongOpt("username")
				.withDescription(
				"Set username for CONNECT TO (default 'DEFAULT_USER').")
				.hasArg().create('y'));
		options.addOption(OptionBuilder.withLongOpt("password")
				.withDescription(
				"Set password for CONNECT TO (default 'nodefault')")
				.hasArg().create('z'));
		options
		.addOption(OptionBuilder
				.withLongOpt("mappingsServer")
				.withDescription(
				"Set the value for the mappings server (default 'jdbc:db2://localhost:50000').")
				.hasArg().create("ms"));
		options
		.addOption(OptionBuilder
				.withLongOpt("addPeers")
				.withArgName("N")
				.withDescription(
				"Specify the number of peers that should be added. (Defaults to 0). See iterations.")
				.hasArg().create("ap"));
		options
		.addOption(OptionBuilder
				.withLongOpt("deletePeers")
				.withArgName("N")
				.withDescription(
						"Specify the number of peers that should be deleted. "
						+ "(Defaults to 0). Each peer will be randomly selected, "
						+ "but a peer won't be added then deleted in the same "
						+ "iteration. See --iterations.")
						.hasArg().create("dp"));
		options
		.addOption(OptionBuilder
				.withLongOpt("addBypasses")
				.withArgName("N")
				.withDescription(
				"Specify the number of bypasses to add on each iteration. (Defaults to 0). See --iterations.")
				.hasArg().create("ab"));
		options
		.addOption(OptionBuilder
				.withLongOpt("deleteBypasses")
				.withArgName("N")
				.withDescription(
				"Specify the number of bypasses to delete on each iteration. (Defaults to 0). See --iterations.")
				.hasArg().create("db"));

		options
		.addOption(OptionBuilder
				.withLongOpt("iterations")
				.withArgName("N")
				.withDescription(
				"The number of times to perform the specified addPeer, deletePeer, addBypass, and deleteBypass operations. (Defaults to 0).")
				.hasArg().create("it"));
		options
		.addOption(OptionBuilder
				.withLongOpt("inout")
				.withDescription(
				"Create _L/_R relations in schema file")
				.create('g')); // no other letters left
		options
		.addOption(OptionBuilder
				.withLongOpt("addValueAttr")
				.withDescription(
				"Add a VALUE attribute, to hold a value in a particular semiring to be used for semiring evaluation")
				.create('q')); // no other letters left
		return options;
	}

	private static void check(String p1, String p2, String op,
			Map<String, Object> params) {
		Integer v1 = (Integer) params.get(p1), v2 = (Integer) params.get(p2);
		boolean flag = true;
		if (">".equals(op)) {
			flag = v1 > v2;
		} else if (">=".equals(op)) {
			flag = v1 >= v2;
		} else if ("<".equals(op)) {
			flag = v1 < v2;
		} else if ("<=".equals(op)) {
			flag = v1 <= v2;
		}
		if (!flag) {
			System.out.println("Error: you specified " + p1 + " = " + v1 + ", "
					+ p2 + " = " + v2 + ", but we require " + p1 + " " + op
					+ " " + p2);
			System.exit(-1);
		}
	}

	public static Map<String,Object> parseCommandLine(String[] args) throws ParseException {
		// create the command line parser
		CommandLineParser parser = new BasicParser();

		// create the Options
		Options options = buildOptions();

		// parse the command line arguments
		CommandLine line = parser.parse(options, args);
		Map<String, Object> params = buildParams(line);

		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Generator", buildOptions());
			System.exit(0);
		}
		return params;
	}

	
	/**
	 * Here's what happens in each iteration:
	 * <ol>
	 * <li>Add in new peers.</li>
	 * <li>Delete peers, but not any added in step 1.</li>
	 * <li>Add bypasses to the graph that steps 1 and 2 generated.</li>
	 * <li>Delete bypasses from the graph that steps 1 and 2, but not 3,
	 * generated.</li>
	 * </ol>
	 * It may have been equivalent, and simpler, to do it like this:
	 * <ol>
	 * <li>Delete peers.</li>
	 * <li>Add peers.</li>
	 * <li>Delete bypasses.</li>
	 * <li>Add bypasses.</li>
	 * </ol>
	 * 
	 * @param args command line arguments. See <code>buildOptions(...)</code>.
	 */
	public static void main(String[] args) {
		try {
			Map<String, Object> params = parseCommandLine(args);
			Generator generator = new Generator(params, (Boolean)params.get("inout"));
			List<List<Integer>> cycles = new ArrayList<List<Integer>>();
			generator.generate();
			MetadataXml metadataXml = new MetadataXml(params, (Boolean)params.get("inout"));
			metadataXml.metadataXml(generator.getLogicalSchemas(), generator
					.getPeers(), generator.getMappings());

			Generator curGenerator = generator;
			Generator prevGenerator = null;
			for (int i = 0; i < (Integer) params.get("iterations"); i++) {
				prevGenerator = curGenerator;
				curGenerator = new Generator(params, (Boolean)params.get("inout"), prevGenerator
						.getPeers().size(), prevGenerator.getPeers().size()
						+ (Integer) params.get("addPeers"), prevGenerator);

				curGenerator.generate(i + 1);
				curGenerator.deletePeers((Integer) params.get("deletePeers"));
				curGenerator.addAndDeleteBypasses((Integer) params
						.get("addBypasses"), (Integer) params
						.get("deleteBypasses"));

				metadataXml.metadataXml(curGenerator.getLogicalSchemas(),
						curGenerator.getPeers(), curGenerator.getMappings(),
						String.valueOf(i + 1));

				// System.out.println("logical schemas: "
				// + curGenerator.getLogicalSchemas() + "\n" + "peers: "
				// + curGenerator.getPeers() + "\n");
				// for (int j = 0; j < curGenerator._mappings.size(); j++) {
				// System.out.println("mappings[" + j + "]: "
				// + curGenerator._mappings.get(j));
				// }
			}
			curGenerator.create(curGenerator);
			curGenerator.fill();
			curGenerator.destroy();
			curGenerator.cycles(cycles);

			curGenerator._journal.write(new FileWriter(params.get("filename")
					.toString()
					+ ".schemaDeltas"), params);
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			System.exit(-1);
		}
	}
}
