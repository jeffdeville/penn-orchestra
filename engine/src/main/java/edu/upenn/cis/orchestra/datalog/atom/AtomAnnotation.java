package edu.upenn.cis.orchestra.datalog.atom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.BoolType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.LongType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.TrustConditions.Trusts;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;

public class AtomAnnotation {
	public enum SEMIRING {BOOLEAN, MINTRUST, MAXTRUST, RANKS, TRANSACTION};
	
	public AtomAnnotation(String label, String varName, SEMIRING sring) {
		_sring = sring;
		switch (sring) {
//		case BOOLEAN:
//			_dataType = new BoolType(false, false);
//			_functionToCompute = "AND";
//			break;
		case MINTRUST:
		case MAXTRUST:
			_dataType = new IntType(false, false);
			_isKey = false;
			break;
		case RANKS:
			_dataType = new DoubleType(false, false);
			_isKey = false;
			break;
		case TRANSACTION:
			break;
		default:
			throw new RuntimeException("Unsupported annotation type");
		}
		_label = label;
	}
	
	public Type getDataType() {
		return _dataType;
	}
	public String getLabel() {
		return _label;
	}
	public String getFunctionToCompute() {
		return _functionToCompute;
	}
	
	/**
	 * TODO: Take the Trust condition map and determine a means of evaluating it,
	 * returning this as a (built-in) atom
	 * 
	 * @param outputVar
	 * @param tc
	 * @return
	 */
	public Atom setTrustConditions(Map<String,Schema> builtins, AtomVariable outputArg, Map<Integer,Set<Trusts>> tc) {
		_tcs = tc;
		throw new RuntimeException("Trust condition conversion not implemented");
	}

	/**
	 * Take the Trust condition map and return the maximum trust value,
	 * ignoring the trust conditions per se
	 * 
	 * @param tc Trust condition map
	 * @return
	 */
	public AtomConst setTrustPriority(Map<Integer,Set<Trusts>> tc) {
		int priority = -1;
		for (Integer i : tc.keySet()) {
			if (priority < i) {
				priority = i;
			}
		}
		AtomConst ret = new AtomConst(new Integer(priority));
		ret.setType(_dataType);
		return ret;
	}

	/**
	 * Return a literal representing the default trust value in a semiring
	 * 
	 * @return
	 */
	public String getDefaultTrustValue() {
		return "1";
	}
	
	public String getDefaultUntrustedValue() {
		return "0";
	}

	/**
	 * Return an atom representing the derivation of trust in a semiring
	 * @param output
	 * @param vars
	 * @return
	 */
	public Atom setTrustDerivation(Map<String,Schema> builtins, AtomVariable output, 
			List<AtomArgument> vars) throws RelationNotFoundException {
		
		String schema;
		String fn = null;
		schema = "ARITH";
		switch (_sring) {
		case MINTRUST:
			fn = "INTLEAST" + (vars.size() );
			break;
		case MAXTRUST:
			fn = "INTGREATEST" + (vars.size() );
			break;
		case RANKS:
			fn = "DBLSUM" + (vars.size() );
			break;
		case TRANSACTION:
		default:
			throw new RuntimeException("Unsupported annotation type");
		}
		
		Schema sch = builtins.get(schema);
		if (sch == null)
			throw new RelationNotFoundException("Missing built-in schema " + schema);
		Relation r = sch.getRelation(fn);
		
		List<AtomArgument> newParms = new ArrayList<AtomArgument>();
		newParms.add(output);
		newParms.addAll(vars);

		return new Atom(null, sch, r, newParms);
	}

	Type _dataType;
	String _label;
	String _varName;
	String _functionToCompute;
	Map<Integer,Set<Trusts>> _tcs = null;
	boolean _isKey;
	SEMIRING _sring;
}
