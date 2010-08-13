package edu.upenn.cis.orchestra.proql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * A subgraph is a (projection of) a schema graph, but has a root node
 * 
 * @author zives
 *
 */
public class Subgraph extends SchemaGraph {
	SchemaGraph _parent;
	TupleNode _rootNode;
	
	public Subgraph(TupleNode root, SchemaGraph parent) {
		super();
		_rootNode = root;
		_parent = parent;
	}
	
	OrchestraSystem getSystem() {
		return _parent._system;
	}
	
	// TODO:  (1) toQuerySet() returns a set of CQs
	//			  but needs a semiring and a set of base values
	
	public String toString() {
		return (_rootNode + ": " + super.toString());
	}
	
	public List<Rule> toQuerySet() {
		List<Rule> ret = new ArrayList<Rule>();
		
		String baseRel = _rootNode.getName();
		
//		Set<String> connectRel = new HashSet<String>();
		Set<DerivationNode> frontier = _derives.get(_rootNode);
		
//		connectRel.add(baseRel);
		
		ITranslationState st = getSystem().getMappingEngine().getState();
		
		List<Rule> allRules = new ArrayList<Rule>();
		allRules.addAll(DeltaRuleGen.getProv2TargetRulesForProvQ(st, getSystem().getMappingDb().getBuiltInSchemas()));
		allRules.addAll(st.getLocal2PeerRules());
		
		for (Rule m : allRules) {
			
			try {
				// See if this is a tuple node we want to traverse
				if (m.getMappingHead().get(0).getRelationContext().toString().equals(baseRel)) {
					for (DerivationNode d : frontier) {
						RelationContext provRel = st.getProvenanceRelationForMapping(d.getName());
						
						for (Atom a : m.getBody())
							if (a.getRelationContext().equals(provRel)) {
								ret.add(m);
								
								// Add the EDB relation rules
								addEDBs(ret, provRel);
								break;
							}
					}
				}
			} catch (MappingNotFoundException mnf) {
				mnf.printStackTrace();
			}
		}
		
		// TODO:
//		expandTree(ret, frontier);

		for (Rule r : ret)
			System.out.println(r);

		return ret;
	}
	
	RelationContext getLocalVersion(List<Rule> s2pRules, RelationContext base) {
		for (Rule m : s2pRules) {
			for (Atom a : m.getBody()) {
				if (a.getRelationContext().toString().equals(base.toString() + "_L"))
					return a.getRelationContext();
			}
		}
		return null;
	}

	/**
	 * Given a mapping relation, add the views defining the mapping relation in terms
	 * of EDBs
	 * 
	 * @param rules
	 * @param mappingRel
	 */
	void addEDBs(List<Rule> rules, RelationContext mappingRel) {
		ITranslationState st = getSystem().getMappingEngine().getState();
		
		for (Rule m: DeltaRuleGen.getSource2ProvRulesForProvQ(st)) {
			if (m.isFakeMapping())
				continue;
			
			if (m.getHead().getRelationContext().equals(mappingRel)) {
				System.out.println("Comparing " + mappingRel + " with " + m.toString());
				
				rules.add(m);
				
//				Atom head = m.getHead();
				
				for (Atom a : m.getBody()) {
					Atom a2 = new Atom(a);
					a2.replaceRelationContext(getLocalVersion(st.getSource2ProvRules(),
							a.getRelationContext()));
					
//					List<Atom> newBody = new ArrayList<Atom>();
//					newBody.add(a2);
					rules.add(new Rule(a, a2, m, getSystem().getMappingDb().getBuiltInSchemas()));
				}
			}
		}
	}
}
