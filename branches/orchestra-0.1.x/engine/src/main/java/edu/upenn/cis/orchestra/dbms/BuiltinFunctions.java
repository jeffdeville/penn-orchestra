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
package edu.upenn.cis.orchestra.dbms;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.SqlFactories;

/**
 * SQL expressions corresponding to "built in functions"
 * 
 * @author zives
 *
 */
public class BuiltinFunctions {
	private static Map<String,Schema> _builtIns;
	private BuiltinFunctions() {}
	private static final ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();

	public static void setBuiltins(Map<String,Schema> builtIns) {
		_builtIns = builtIns;
	}

	/**
	 * Is this schema / function combo something built in?
	 * 
	 * @param sch
	 * @param fn
	 * @return
	 */
	public static boolean isBuiltIn(String sch, String fn) {
		if (!_builtIns.keySet().contains(sch))
			return false;

		if (_builtIns.get(sch).getIDForName(fn) == -1)
			return true;
		
		return true;
	}
	
	public static boolean isBuiltIn(String sch, String fn, Map<String,Schema> builtIns) {
		if (!builtIns.keySet().contains(sch))
			return false;

		if (builtIns.get(sch).getIDForName(fn) == -1)
			return true;
		
		return true;
	}
	
	/**
	 * Determines whether a relation is a built-in function of {@code builtIns}.
	 * 
	 * @param a
	 * @param builtIns 
	 * @return {@code true} if {@code a} is a built-in function present in {@code builtIns}
	 */
	public static boolean isBuiltInAtom(Atom a, Map<String, Schema> builtIns) {
		return BuiltinFunctions.isBuiltIn(a.getSchema().getSchemaId(), a
				.getRelation().getName(), builtIns);
	}
	
	/**
	 * Determines whether a relation is a built-in function.
	 * 
	 * @param a
	 * @return {@code true} if {@code a} is a built-in function present in these {@code BuiltInFunctions}
	 */
	public static boolean isBuiltInAtom(Atom a) {
		return BuiltinFunctions.isBuiltInAtom(a, _builtIns);
	}
	
	public static ISqlExpression getExpression(String sch, String fn, List<ISqlExp> args) {
		if (sch.equals("COMPARE")) {
			if (fn.equals("INTLESS")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.LT, args.get(0), args.get(1));
			} else if (fn.equals("INTLESSEQUAL")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.LTE, args.get(0), args.get(1));
			} else if (fn.equals("INTGREATER")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.GT, args.get(0), args.get(1));
			} else if (fn.equals("INTGREATEREQUAL")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.GTE, args.get(0), args.get(1));
			} else if (fn.equals("INTEQUAL")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.EQ, args.get(0), args.get(1));
			} else if (fn.equals("INTNOTEQUAL")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.NEQ, args.get(0), args.get(1));
			} else if (fn.equals("STRLIKE")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.LIKE, args.get(0), args.get(1));
			}  
		} else if (sch.equals("ARITH")) {
			if (fn.equals("INTADD")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.PLUSSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTSUB")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.MINUSSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTMUL")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.MULTSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTDIV")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.DIVSIGN, args.get(0), args.get(1));
			}  
		} else if (sch.equals("STRING")) {
			if (fn.equals("STRCAT")) {
				return _sqlFactory.newExpression(ISqlExpression.Code.PIPESSIGN, args.get(0), args.get(1));
				/*
			} else if (fn.equals("SUBSTR")) {
				return _sqlFactory.newSqlExpression(ISqlExpression.Code.)
			} else if (fn.equals("STRLEN")) {
				return "LENGTH(" + args.get(1) + ")";
				*/
			}  
		}
		else if (sch.equals("EQUALITYUDFSL")|| sch.equals("EQUALITYUDFSR")){
			ISqlExpression s = _sqlFactory.newExpression(ISqlExpression.Code._NOT_SUPPORTED, args.get(0));
			s.setOperator(fn);
			for(int i=1;i<args.size();i++)
				s.addOperand(args.get(i));
			return s;
			
		}
		return null;
	}
	
	public static String getResultString(String sch, String fn, List<String> args) {
		if (sch.equals("COMPARE")) {
			if (fn.equals("INTLESS")) {
				return "(" + args.get(0) + " < " + args.get(1) + ")";
			} else if (fn.equals("INTLESSEQUAL")) {
				return "(" + args.get(0) + " <= " + args.get(1) + ")";
			} else if (fn.equals("INTGREATER")) {
				return "(" + args.get(0) + " > " + args.get(1) + ")";
			} else if (fn.equals("INTGREATEREQUAL")) {
				return "(" + args.get(0) + " >= " + args.get(1) + ")";
			} else if (fn.equals("INTEQUAL")) {
				return "(" + args.get(0) + " = " + args.get(1) + ")";
			} else if (fn.equals("INTNOTEQUAL")) {
				return "(" + args.get(0) + " <> " + args.get(1) + ")";
			} else if (fn.equals("STRLIKE")) {
				return args.get(0) + " LIKE '" + args.get(1) + "'";
			}  
		} else if (sch.equals("ARITH")) {
			if (fn.equals("INTADD")) {
				return "(" + args.get(0) + " + " + args.get(1) + ")";
			} else if (fn.equals("INTSUB")) {
				return "(" + args.get(0) + " - " + args.get(1) + ")";
			} else if (fn.equals("INTMUL")) {
				return "(" + args.get(0) + " * " + args.get(1) + ")";
			} else if (fn.equals("INTDIV")) {
				return "(" + args.get(0) + " / " + args.get(1) + ")";
			}  
		} else if (sch.equals("STRING")) {
			if (fn.equals("STRCAT")) {
				return "(" + args.get(0) + " || " + args.get(1) + ")";
			} else if (fn.equals("SUBSTR")) {
				return "SUBSTR(" + args.get(0) + "," + args.get(1) + "," + args.get(2) + ")";
			} else if (fn.equals("STRLEN")) {
				return "LENGTH(" + args.get(0) + ")";
			}  
		} else if(sch.equals("EQUALITYUDFSL")|| sch.equals("EQUALITYUDFSR")){
			StringBuffer arglist= new StringBuffer();
			arglist.append("(");
			if(args.size()>0)
				arglist.append(args.get(0));
			for(int i=1;i<args.size();i++)
			{
				arglist.append(",");
				arglist.append(args.get(i));
			}
			arglist.append(")");
			return fn +arglist.toString();
		}
		return "";
	}

	public static String evaluateBuiltIn(Atom atom, Map<String,String> varmap,
			Map<String,ISqlExpression> whereExpressions, Set<ISqlExpression> whereRoots, List<Atom> allAtoms) {
		String sch = atom.getSchema().getSchemaId();
		String fn = atom.getRelation().getName();
		
		List<String> args = new ArrayList<String>();
		List<ISqlExp> children = newArrayList();
		//args.add(atom.getValues().get(0).toString());
		
		// Make sure the parameters are defined
		
		for (int i = 0/*getFirstParm(sch, fn)*/; i < atom.getValues().size(); i++) {
			String arg = atom.getValues().get(i).toString();
			if (atom.getValues().get(i) instanceof AtomConst) {
				args.add(arg);
				try {
					Integer.valueOf(arg);
					children.add(_sqlFactory.newConstant(arg, ISqlConstant.Type.NUMBER));
				} catch (NumberFormatException ne) {
					
					// Trim '' because the parent class already adds these to a string
					if (arg.charAt(0) == '\'' && arg.charAt(arg.length() - 1) == '\'') {
						arg = arg.substring(1, arg.length() - 1);
					}
					children.add(_sqlFactory.newConstant(arg, ISqlConstant.Type.STRING));
				}
			} else if (varmap.get(arg) == null && i >= getFirstParm(sch, fn))
				return "";
			else if (i >= getFirstParm(sch, fn)) {
				args.add(getArgumentForVar(fn,arg,i,varmap,allAtoms));
				if (whereExpressions.get(arg) != null)
					children.add(whereExpressions.get(arg));
				else
					children.add(_sqlFactory.newConstant(getArgumentForVar(fn,arg,i,varmap,allAtoms), ISqlConstant.Type.COLUMNNAME));
				// This is no longer the root of a condition -- it's a subexpression
//				_whereRoots.remove(_whereExpressions.get(arg));
			}
		}
		
		ISqlExpression newExpr = BuiltinFunctions.getExpression(sch, fn, children);
		
		if (newExpr.isBoolean())
			whereRoots.add(newExpr);
		else
			whereExpressions.put(atom.getValues().get(0).toString(), newExpr);
		
		return BuiltinFunctions.getResultString(sch, fn, args);
	}
	
	protected static String getArgumentForVar(String fn,String arg, int pos,Map<String, String> varmap, List<Atom> allAtoms){
		if(!UDFunctions.isUDF(fn))
			return varmap.get(arg);
		else{
			Relation rel = UDFunctions.argumentFor(fn, new Integer(pos), null);
			RelationField rf = UDFunctions.argumentFor(fn, new Integer(pos));
			int atomIndex=0;
			for(Atom a:allAtoms){
				if(a.getRelation().equals(rel))
					break;
				atomIndex++;
			}
			return RuleSqlGen.fullNameForAttr(rf, atomIndex);
		}
	}
	protected static int getFirstParm(String sch, String fn) {
		if (sch.equals("COMPARE"))
			return 0;
		else
			return 1;
	}
}
