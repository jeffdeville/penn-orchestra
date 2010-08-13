package edu.upenn.cis.orchestra.proql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;

/**
 * A subgraph is a (projection of) a schema graph, but has a root node
 * 
 * @author zives
 *
 */
public class SchemaSubgraph extends SchemaGraph {
	SchemaGraph _parent;
	TupleNode _rootNode;

	public SchemaSubgraph(TupleNode root, SchemaGraph parent) {
		super();
		_rootNode = root;
		_parent = parent;
	}

	OrchestraSystem getSystem() {
		return _parent._system;
	}

	// TODO:  (1) toQuerySet() returns a set of CQs
	//			  but needs a semiring and a set of base values

	public TupleNode getRootNode() {
		return _rootNode;
	}

	public String toString() {
		return (_rootNode + ": " + super.toString());
	}

	public Set<String> getNextLevel(DerivationNode d, ITranslationState st) {
		Set<String> frontierContexts = new HashSet<String>();
		for (TupleNode t : d.getSources()) {
			Set<DerivationNode> nextLevel = _derives.get(t);
			if (nextLevel != null) {
				for (DerivationNode d2: nextLevel) {
					try {
						RelationContext rc = st.getProvenanceRelationForMapping(d2.getName());
						frontierContexts.add(rc.toString());
						//						System.err.println("Expanding next level " + d2.getName() + " to actual relations " + rc);//st.getProvenanceRelationForMapping(d2.getName()).toString());
					} catch (MappingNotFoundException mnf) {
						mnf.printStackTrace();
					}
				}
			}
		}
		return frontierContexts;
	}

	public List<Rule> toQuerySet() {
		List<Rule> ret = new ArrayList<Rule>();

		String baseRel = _rootNode.getName();

		//		Set<String> connectRel = new HashSet<String>();
		Set<DerivationNode> frontier = _derives.get(_rootNode);

		//		connectRel.add(baseRel);

		ITranslationState st = getSystem().getMappingEngine().getState();

		boolean first = true;
		Rule last = null;

		List<Rule> allRules = new ArrayList<Rule>();
		allRules.addAll(DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb().getBuiltInSchemas()));
		allRules.addAll(st.getLocal2PeerRules());

		for (Rule m : allRules) {
			// See if this is a tuple node we want to traverse
			if (m.getHead().getRelationContext().toString().equals(baseRel)) {
				if (first) {
					Atom a2 = new Atom(m.getHead());
					if(a2.getRelation().hasLocalData()){
						a2.replaceRelationContext(getLocalVersion(st.getLocal2PeerRules(),
								m.getHead().getRelationContext()));

						//ret.add(
						last = (new Rule(m.getHead(), a2, m, getSystem().getMappingDb().getBuiltInSchemas()));
					}
					first = false;
					////					for (Rule m : DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb())) {
					//					// See if this is a tuple node we want to traverse
					//					if (m.getHead().getRelationContext().toString().equals(baseRel)) {
					//					expandTree(ret, frontier, m, st);
					//					}
				}
				expandTree(ret, frontier, m, st);
			}
		}

		if (last != null)
			ret.add(last);

		for (Rule r : ret)
			Debug.println(r.toString());

		return ret;
	}

	/**
	 * Takes a given relation context and finds the "_L" version of it from a set of rules
	 * with atoms including the _L in their body.  This saves us from creating a new object. 
	 * 
	 * @param s2pRules
	 * @param base
	 * @return
	 */
	RelationContext getLocalVersion(Collection<Rule> s2pRules, RelationContext base) {
		for (Rule m : s2pRules) {
			for (Atom a : m.getBody()) {
				if (a.getRelationContext().toString().equals(base.toString() + "_L"))
					return a.getRelationContext();
			}
		}
		return null;
	}

	void expandTree(Collection<Rule> ret, Collection<DerivationNode> frontier, Rule m, ITranslationState st) {
		Debug.println("FRONTIER -- RULE:" + m);
		for(DerivationNode f : frontier)
			Debug.println(f.toString());

		for (DerivationNode d : frontier) {

			RelationContext provRel;
			try {
				provRel = st.getProvenanceRelationForMapping(d.getName());
			} catch (MappingNotFoundException mnf) {
				mnf.printStackTrace();
				continue;
			}

			Set<String> nextLevel = getNextLevel(d, st);

			boolean addedSomething = false;
			for (Atom a : m.getBody()){
				if (a.getRelationContext().equals(provRel)) {
					if(!ret.contains(m)){
						ret.add(m);
					}else{
						Debug.println("Rule already there: " + m);
					}

					// Add the EDB relation rules
					addedSomething = addEDBandIDBs(ret, provRel, nextLevel);
					break;
				}
			}
			if(addedSomething){
				for (TupleNode tup: d.getSources()) {
					Set<DerivationNode> frontier2 = _derives.get(tup); 
					if (frontier2 != null){

						List<Rule> allRules = new ArrayList<Rule>();
						allRules.addAll(DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb().getBuiltInSchemas()));
						//					allRules.addAll(st.getLocal2PeerRules());

						for (Rule m2 : allRules) {
							// See if this is a tuple node we want to traverse
							if (m2.getHead().getRelationContext().toString().equals(tup.getName())) {
								expandTree(ret, frontier2, m2, st);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Given a mapping relation, add the views defining the mapping relation in terms
	 * of EDBs
	 * 
	 * @param rules
	 * @param mappingRel
	 */
	public boolean addEDBandIDBs(Collection<Rule> rules, RelationContext mappingRel, Set<String> frontierContexts) {
		boolean didSomething = false;
		ITranslationState st = getSystem().getMappingEngine().getState();
		//		for (Rule m: st.getSource2ProvRules()) {
		for (Rule m: DeltaRuleGen.getSource2ProvRulesForProvQ(st)) {
			if (m.isFakeMapping())
				continue;

			if (m.getHead().getRelationContext().equals(mappingRel)) {
				if(!rules.contains(m)){
					//					Debug.println("Adding " + mappingRel + " rule: " + m.toString());
					Debug.println("Adding rule: " + m.toString());
					rules.add(m);
					didSomething = true;
				}else{
					Debug.println("Rule already there: " + m);
				}


				// Create the EDB mapping (from the atom to its _L table)
				for (Atom a : m.getBody()) {
					//					Atom a2 = new Atom(a);

					if(a.getRelation().hasLocalData()){
						for(Rule r : st.getLocal2PeerRules()){
							if(r.getHead().getRelationContext().equals(a.getRelationContext())){
								if(!rules.contains(r)){
									Debug.println("Adding rule: " + r.toString());
									rules.add(r);
									didSomething = true;
								}else{
									Debug.println("Rule already there: " + r);
								}
							}
						}

						//						a2.replaceRelationContext(getLocalVersion(st.getLocal2PeerRules(),
						//								a.getRelationContext()));
						//
						//						Rule newRule = new Rule(a, a2, m, getSystem().getMappingDb());				
						//						Debug.println("Adding " + a.getRelationContext() + 
						//								" rule: " + newRule.toString());
						//
						//						if(!rules.contains(newRule)){
						//							rules.add(newRule);
						//						}else{
						//							Debug.println("EDB2 Rule already there: " + m);
						//						}
					}
					//					System.out.println("Looking for the useful mappings from Mx to " + a.getRelationContext());
					//					System.out.println(" Where 'useful' includes " + frontierContexts);
					// Now we must find which rules to add to expand the atom

					for (Rule m2: DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb().getBuiltInSchemas())) {
						//						for (Rule m2: DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb())) {
						//						if (m2.getBody().size() != 1)
						//						continue;

						//						System.err.println(" rule: " + m2.toString() + 
						//						((m2.getHead().getRelationContext().equals(a.getRelationContext())) ? "MATCH ": " ") +
						//						((frontierContexts.contains(m2.getBody().get(0).getRelationContext().toString())) ? "MATCH ": " "));

						if (m2.getHead().getRelationContext().equals(a.getRelationContext()) &&
								frontierContexts.contains(m2.getBody().get(0).getRelationContext().toString())) {

							if(!rules.contains(m2)){
								//								Debug.println("Adding " + a.getRelationContext() + " rule: " + m2.toString());
								Debug.println("Adding rule: " + m2.toString());
								rules.add(m2);
								didSomething = true;
							}else{
								Debug.println("Rule already there: " + m2);
							}

						}						
					}
				}
			}
		}
		return didSomething;
	}
}
