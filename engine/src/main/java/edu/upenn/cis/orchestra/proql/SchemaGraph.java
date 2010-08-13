package edu.upenn.cis.orchestra.proql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;

public class SchemaGraph {
	Map<TupleNode,Set<DerivationNode>> _derivedFrom;
	Map<TupleNode,Set<DerivationNode>> _derives;
	OrchestraSystem _system = null;
	
	Set<DerivationNode> _derivations;
	
	Set<TupleNode> _allTuples;
	
	public SchemaGraph(OrchestraSystem system) {
		this();
		_system = system;
		addSystem(system);
	}
	
	/**
	 * A schema graph consists of a set of derivations that contain tuple nodes.
	 * Derivations "connect" when one derives from another.  A derivation goes "from"
	 * a set of tuple nodes "to" a set of tuple nodes
	 */
	public SchemaGraph() {
		_derivedFrom = new HashMap<TupleNode,Set<DerivationNode>>();
		_derives = new HashMap<TupleNode,Set<DerivationNode>>();
		_derivations = new HashSet<DerivationNode>();
		
		_allTuples = new HashSet<TupleNode>();
	}

	/**
	 * Add in all of the mappings from a CDS system
	 * 
	 * @param system
	 */
	public void addSystem(OrchestraSystem system) {
		
//		Add all "real" mappings to the schema graph 
		for (Mapping m : system.getAllSystemMappings(true)) {
//		HashSet<Mapping> usefulRules = new HashSet<Mapping>();
//		usefulRules.addAll(system.getMappingEngine().getState().getSource2ProvRules());
//		usefulRules.addAll(system.getMappingEngine().getState().getProv2TargetMappings());
//		usefulRules.addAll(system.getMappingEngine().getState().getLocal2PeerRules());
//		for (Mapping m : usefulRules) {
			DerivationNode d = new DerivationNode(m);//body, m.getId(), head);
//			System.err.println("Adding " + d.toString());
			addDerivationNode(d);
		}
//      Also add the local-to-peer rules to the schema graph
		for (Mapping m : system.getMappingEngine().getState().getLocal2PeerRules()){
			DerivationNode d = new DerivationNode(m);//body, m.getId(), head);
			addDerivationNode(d);
		}
	}
	
	public Set<TupleNode> getAllTuples() {
		return _allTuples;
	}
	
	public Set<DerivationNode> getAllDerivations() {
		return _derivations;
	}
	
	/**
	 * Adds a derivation node to the graph
	 * @param d
	 */
	public void addDerivationNode(DerivationNode d) {
		_derivations.add(d);
		for (TupleNode t: d.getSources()) {
			Set<DerivationNode> dset = _derivedFrom.get(t);
			
			if (dset == null) {
				dset = new HashSet<DerivationNode>();
				_derivedFrom.put(t, dset);
			}
			
			dset.add(d);
			
			_allTuples.add(t);
		}

		for (TupleNode t: d.getTargets()) {
			Set<DerivationNode> dset = _derives.get(t);
			
			if (dset == null) {
				dset = new HashSet<DerivationNode>();
				_derives.put(t, dset);
			}
			
			dset.add(d);
			_allTuples.add(t);
		}
	}
	
	/**
	 * The set of derivations of this node
	 * 
	 * @param t
	 * @return
	 */
	public Set<DerivationNode> getSourceDerivations(TupleNode t) {
		if (_derivedFrom.containsKey(t))
			return _derivedFrom.get(t);
		else
			return new HashSet<DerivationNode>();
	}
	
	/**
	 * The set of derivations *from* this node
	 * @param t
	 * @return
	 */
	public Set<DerivationNode> getTargetDerivations(TupleNode t) {
		if (_derives.containsKey(t))
			return _derives.get(t);
		else
			return new HashSet<DerivationNode>();
	}
	
	/**
	 * We have an EDB if it doesn't derive from anything
	 * 
	 * @param t
	 * @return
	 */
	public boolean isEdb(TupleNode t) {
		if (_derives.containsKey(t))
			return _derives.get(t).isEmpty();
		else
			return false;
	}
	
	public String toString() {
		StringBuffer ret = new StringBuffer("{");
		
		for (DerivationNode d : _derivations)
			ret.append(d.toString() + "\n");
		
		ret.append("}");
		return new String(ret);
	}
}
