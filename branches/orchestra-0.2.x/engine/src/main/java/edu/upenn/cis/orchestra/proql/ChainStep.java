package edu.upenn.cis.orchestra.proql;

import java.util.Collection;
import java.util.Set;

import edu.upenn.cis.orchestra.proql.PatternStep.MATCH_RESULT;

public class ChainStep extends PatternStep {

	public ChainStep(Pattern p, String varBinding) {
		super(p, varBinding);
	}
	
	public ChainStep(Pattern p, 
			Collection<TupleMatch> targets, Collection<TupleMatch> sources, String varBinding) {
		super(p, varBinding);
		
		for (TupleMatch t : sources)
			addSourceMatch(t);

		for (TupleMatch t: targets)
			addTargetMatch(t);
	}
	
	public MATCH_RESULT match(DerivationNode d, //Set<DerivationNode> visited,
			Set<TupleNode> connectors) {
		
		boolean isAMatch = true;
		/*
		for (TupleMatch srcMatch : _targetNodes) {
			Set<TupleNode> matches = srcMatch.matchesTargets(d); 
			if (matches.isEmpty())
				isAMatch = false;
		}
		if (!isAMatch)
			return MATCH_RESULT.NOMATCH_NOADVANCE;
		*/
		
		for (TupleMatch srcMatch : _sourceNodes) {
			Set<TupleNode> matches = srcMatch.matchesSources(d); 
			if (matches.isEmpty())
				isAMatch = false;
			else
				connectors.addAll(matches);
		}
		
		if (isAMatch)
			return MATCH_RESULT.MATCH_NOADVANCE;
		else
			return MATCH_RESULT.NOMATCH_NOADVANCE;
	}
	
	public String getStepString() {
		return "**";
	}
}
