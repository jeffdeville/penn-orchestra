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
package edu.upenn.cis.orchestra.datalog.atom;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.PositionedString;

public class UntypedAtom {
	
	protected String m_name;
	protected List<AtomArgument> m_values;
	
	public UntypedAtom(String name, List<AtomArgument> values) {
		m_name = name;
		m_values = values;
	}
	
	public synchronized String getName() {
		return m_name;
	}

	public synchronized void setName(String name) {
		m_name = name;
	}

	public synchronized List<AtomArgument> getValues() {
		return m_values;
	}
	
	public synchronized void setValues(List<AtomArgument> values) {
		m_values = values;
	}

	protected static boolean isInteger(String arg) {
		try {
			Integer.parseInt(arg);
			return true;
		} catch (NumberFormatException e) {
		}
		return false;
	}
	
	protected static boolean isDouble(String arg) {
		try {
			Float.parseFloat(arg);
			return true;
		} catch (NumberFormatException e) {
		}
		return false;
	}
	
	protected static void complain(PositionedString str, String expected) throws ParseException {
		throw new ParseException("Expected " + expected + " in string \'" + str.getString() 
				+ "\' at position " + str.getPosition(), str.getPosition());
	}
	
	public static UntypedAtom parseInterpretedPredicate(PositionedString str, Holder<Integer> counter) throws ParseException {
		str.skipWhitespace();
		if (!str.inRange()) {
			complain(str, "atom");
		}

		// parse the arguments
		ArrayList<AtomArgument> values = new ArrayList<AtomArgument>();
		
		String namepat = "[a-zA-Z_][\\w\\.:]*";
		String constpat = "'([^']|\\\\.|'')*'";
		String floatpat = "\\d+.\\d+";
		String intpat = "\\d+";
		String nullpat = "-";
		String dontcarepat = "_";
		Matcher mat = str.match("(" + namepat + ")|(" + constpat + ")|(" + floatpat + ")|(" + intpat + ")|(" + nullpat + ")|(" + dontcarepat + ")");
		AtomArgument val;
		if (mat == null) {
			complain(str, "variable or constant");
			val = null;	// unreachable, just to keep compiler happy
		} else if (mat.group(1) != null) {
			// variable
			val = new AtomVariable(mat.group(1));
		} else if (mat.group(2) != null) {
			// constant
			String arg = mat.group(2);
			val = new AtomConst(arg.substring(1, arg.length()-1));
			val.setType(new StringType(true, true, true, arg.length()-2));
		} else if (mat.group(3) != null) {
			// floating point constant
			val = new AtomConst(mat.group(3));
			val.setType(DoubleType.DOUBLE);
		} else if (mat.group(4) != null) {
			// integer constant
			val = new AtomConst(mat.group(4));
			val.setType(IntType.INT);
		} else if (mat.group(5) != null) {
			// null
			val = new AtomConst((String)null);
		} else {
			// fresh variable
			assert(mat.group(6) != null);
			val = new AtomVariable("a:v" + counter.value++);
		}
		values.add(val);
		
		str.skipWhitespace();
		
		String virtualRel = "";
		if (str.isChar('<')) {
			str.increment();
			if (str.isChar('='))
				virtualRel = "lessEqual";
			else
				virtualRel = "lessThan";
		} else if (str.isChar('>')) {
			str.increment();
			if (str.isChar('='))
				virtualRel = "greaterEqual";
			else
				virtualRel = "greaterThan";
		} else if (str.isChar('=')) {
			virtualRel = "equal";
		} else
			complain(str, "operator");
		str.skipWhitespace();
		if (!str.inRange())
			complain(str, "operand");

		if (mat == null) {
			complain(str, "variable or constant");
			val = null;	// unreachable, just to keep compiler happy
		} else if (mat.group(1) != null) {
			// variable
			val = new AtomVariable(mat.group(1));
		} else if (mat.group(2) != null) {
			// constant
			String arg = mat.group(2);
			val = new AtomConst(arg.substring(1, arg.length()-1));
			val.setType(new StringType(true, true, true, arg.length()-2));
		} else if (mat.group(3) != null) {
			// floating point constant
			val = new AtomConst(mat.group(3));
			val.setType(DoubleType.DOUBLE);
		} else if (mat.group(4) != null) {
			// integer constant
			val = new AtomConst(mat.group(4));
			val.setType(IntType.INT);
		} else if (mat.group(5) != null) {
			// null
			val = new AtomConst((String)null);
		} else {
			// fresh variable
			assert(mat.group(6) != null);
			val = new AtomVariable("a:v" + counter.value++);
		}
		values.add(val);
		str.skipWhitespace();

		return new UntypedAtom(virtualRel, values);
	}
	
	public static UntypedAtom parse(PositionedString str, Holder<Integer> counter) throws ParseException {
		str.skipWhitespace();
		if (!str.inRange()) {
			complain(str, "atom");
		}
		// parse the name of the atom
		String namepat = "[a-zA-Z_][\\w\\.:<>]*";
		Matcher name = str.match(namepat);
		if (name == null) {
			complain(str, "relation name");
		}
		str.skipWhitespace();
		if (!str.skipString("(")) {
			complain(str, "open parenthesis");
		}
		// parse the arguments
		ArrayList<AtomArgument> values = new ArrayList<AtomArgument>();
		boolean first = true;
		str.skipWhitespace();
		while (str.inRange()) {
			if (str.isChar(')')) {
				break;
			} else if (first) {
				first = false;
			} else if (!str.skipString(",")) {
				complain(str, "comma");
			}
			str.skipWhitespace();
			if (!str.inRange()) {
				break;
			}
			String constpat = "'([^']|\\\\.|'')*'";
			String floatpat = "\\d+\\.\\d+";
			String intpat = "\\d+";
			String nullpat = "-";
			String dontcarepat = "_";
			Matcher mat = str.match("(" + namepat + ")|(" + constpat + ")|(" + floatpat + ")|(" + intpat + ")|(" + nullpat + ")|(" + dontcarepat + ")");
    		AtomArgument val;
			if (mat == null) {
				complain(str, "variable or constant");
				val = null;	// unreachable, just to keep compiler happy
			} else if (mat.group(1) != null) {
				// variable
			    val = new AtomVariable(mat.group(1));
			} else if (mat.group(2) != null || mat.group(3) != null) {
				// constant
				String arg = mat.group(2);
    			val = new AtomConst(arg.substring(1, arg.length()-1));
    			val.setType(new StringType(true, true, true, arg.length()-2));
			} else if (mat.group(4) != null) {
				// floating point constant
    			val = new AtomConst(mat.group(4));
    			val.setType(DoubleType.DOUBLE);
			} else if (mat.group(5) != null) {
				// integer constant
    			val = new AtomConst(mat.group(5));
    			val.setType(IntType.INT);
			} else if (mat.group(6) != null) {
				// null
    			val = new AtomConst((String)null);
			} else {
				// fresh variable
				assert(mat.group(7) != null);
    			val = new AtomVariable("a:v" + counter.value++);
			}
    		values.add(val);
    		str.skipWhitespace();
    	}
		if (!str.inRange() || !str.skipString(")")) {
			complain(str, "close parenthesis");
		}
		return new UntypedAtom(name.group(), values);
	}

    protected static Type getType(AtomArgument val, List<Atom> body) {
    	if (val instanceof AtomVariable) {
    		for (Atom atom : body) {
    			List<AtomArgument> lv = atom.getValues();
    			for (int i = 0; i < lv.size(); i++) {
    				if (lv.get(i).equals(val)) {
    					return atom.getRelation().getField(i).getType();
    				}
    			}
    		}
    	} else if (val instanceof AtomConst) {
    		AtomConst c = (AtomConst)val;
    		return c.getType();
    	} else if (val instanceof AtomSkolem) {
    		assert(false);	// UNIMPLEMENTED
    	}
    	return null;
    }
    
    
    /**
     * Gets the appropriate relation context and type info for the atom,
     * from the main OrchestraSystem catalog
     * 
     * @param catalog
     * @param locals
     * @return
     * @throws ParseException Couldn't parse atom name
     * @throws RelationNotFoundException Couldn't find the definition
     */
    public synchronized Atom getTyped(OrchestraSystem catalog)
    throws ParseException, RelationNotFoundException {
    	return getTyped(catalog, null);
    }

    /**
     * Gets the appropriate relation context and type info for the atom,
     * either from the main OrchestraSystem catalog or from the local
     * definitions
     * 
     * @param catalog
     * @param locals
     * @return
     * @throws ParseException Couldn't parse atom name
     * @throws RelationNotFoundException Couldn't find the definition
     */
    public synchronized Atom getTyped(OrchestraSystem catalog, 
    		Map<String,RelationContext> locals) throws ParseException, RelationNotFoundException {
    	RelationContext rel = null;
    	String[] parts = m_name.split("\\.");
    	int last = parts.length - 1;
    	if (parts.length != 3 && locals != null) {
			rel = locals.get(m_name);

			if (rel == null)
				throw new ParseException("Unknown view or badly formed atom string (expected two dots in relation name): " + m_name, 0);
    	}
    	AtomType a = Atom.getSuffix(parts[last]);
    	String name = Atom.getPrefix(parts[last]);

    	try {
    		if (rel == null)
    			rel = catalog.getRelationByName(parts[0], parts[1], name);
    	} catch (RelationNotFoundException rnf) {
    		if (locals != null) {
    			rel = locals.get(m_name);
    		} else
    			throw rnf;
    	}
    	
    	try {
    		return new Atom(rel, m_values, a);
    	} catch (Exception e) {
    		throw new ParseException("Unable to match definition " + 
    				rel.getRelation().toString() + " to parameter string " + 
    				m_values.toString() + ": " + e.getMessage(), 0);
    	}
    }
    
    public synchronized Atom getTyped(List<Atom> body) throws RelationNotFoundException {
    	ArrayList<String> names = new ArrayList<String>();
    	ArrayList<RelationField> fields = new ArrayList<RelationField>(m_values.size());
    	for (int i = 0; i < m_values.size(); i++) {
    		Type t = getType(m_values.get(i), body);
    		if (t == null) {
    			throw new RelationNotFoundException("Couldn't find type for variable " + m_values.get(i));
    		}
    		RelationField f = new RelationField("F" + i, "", t);
    		fields.add(f);
    		names.add("F" + i);
    	}
    	Relation rel = new Relation("", "", m_name, m_name, "", true, false, fields);
    	try {
    		PrimaryKey key = new PrimaryKey("default", rel, names);
        	rel.setPrimaryKey(key);
        	rel.setLabeledNulls(false);
        	rel.markFinished();
    	} catch (UnknownRefFieldException e) {
    		assert(false);	// can't happen
    	}
    	RelationContext rc = new RelationContext(rel, null, null, false);
    	return new Atom(rc, m_values);
    }    
}

