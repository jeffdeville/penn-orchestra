package edu.upenn.cis.orchestra.proql;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.orchestra.proql.PatternStep.MATCH_RESULT;

public class DerivationStep extends PatternStep {
	String _mapping;

	public DerivationStep(Pattern p, String varBinding) {
		super(p, varBinding);
		
	}
	
	public DerivationStep(Pattern p, Collection<TupleMatch> targets, String mapping,
			Collection<TupleMatch> sources, String varBinding) {
		super(p, varBinding);
		
		for (TupleMatch t : sources)
			addSourceMatch(t);
		_mapping = mapping;
		for (TupleMatch t: targets)
			addTargetMatch(t);
	}
	
	public MATCH_RESULT match(DerivationNode d, //Set<DerivationNode> visited,
			Set<TupleNode> connectors) {
		
		boolean isAMatch = true;
		for (TupleMatch srcMatch : _targetNodes) {
			Set<TupleNode> matches = srcMatch.matchesTargets(d); 
			if (matches.isEmpty())
				isAMatch = false;
		}
		if (!isAMatch)
			return MATCH_RESULT.NOMATCH_ADVANCE;
		
		for (TupleMatch srcMatch : _sourceNodes) {
			Set<TupleNode> matches = srcMatch.matchesSources(d); 
			if (matches.isEmpty())
				isAMatch = false;
			else
				connectors.addAll(matches);
		}
		isAMatch &= d.matches(_mapping);
		
		if (isAMatch)
			return MATCH_RESULT.MATCH_ADVANCE;
		else
			return MATCH_RESULT.NOMATCH_ADVANCE;
	}

	public String getStepString() {
		if (_mapping.isEmpty())
			return "<-";
		else
			return _mapping + " <-";
	}
}
