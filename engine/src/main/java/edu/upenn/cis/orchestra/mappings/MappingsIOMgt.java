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
package edu.upenn.cis.orchestra.mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;

/**
 * 
 * @author gkarvoun
 *
 */
public class MappingsIOMgt 
{
	public static List<Rule> inOutTranslationR(List<Rule> rules, boolean ins){	
		List<Rule> ret = new ArrayList<Rule>();

		if(!Config.getRejectionTables())
			return rules;

		for(Rule r: rules){
			try{
				Atom head = r.getHead();
				Schema s = head.getSchema();
				Peer p = head.getPeer();

//				TableSchema rl = s.getRelation(head.getRelation().getDbRelName() + "_L");
				Relation rr = s.getRelation(head.getRelation().getLocalRejDbName());//.getDbRelName() + "_R");

//				RelationContext rlc = new RelationContext(rl, s, p);
				RelationContext rrc = new RelationContext(rr, s, p, false);
				
				// Ensure we keep ONLY the fields that are not _LN... 
				List<RelationField> fields = new ArrayList<RelationField>();
				fields.addAll(rr.getFields());
				int i = 0; 
				
				while (i < fields.size()) {
					if (Config.useCompactNulls() && fields.get(i).getName().endsWith(RelationField.LABELED_NULL_EXT))
						fields.remove(i);
					else
						i++;
				}
				List<AtomArgument> args = new ArrayList<AtomArgument>();
				for (int j = 0; j < i; j++) {
					args.add(head.getValues().get(j));
				}

//				Add \neg R_R in the body of every mapping with R in the head
				Atom notrra = new Atom(rrc, args);//head.getValues());
				for (int j = 0; j < i; j++)
					if (head.isNullable(j))
						notrra.setIsNullable(j);
				notrra.negate();

				Rule mappR = r.deepCopy();
				if(ins){
					mappR.getBody().add(notrra);	
				}
				ret.add(mappR);
			}catch(RelationNotFoundException e){
				Debug.println("Relation not found! " + e.getMessage());
				e.printStackTrace();
			}
		}
		return ret;
	}

	public static List<Rule> inOutTranslationL(Map<String, Schema> builtInSchemas, 
			List<RelationContext> rels) throws RelationNotFoundException {	
		List<Rule> ret = new ArrayList<Rule>();

//		Create a new mapping from R_L to R			
//		ScMappingAtom rla = new ScMappingAtom(rlc, head.getValues());

		for(RelationContext r : rels){
			Relation rl = null;
			try {
				//rl = r.getSchema().getRelation(r.getRelation().getDbRelName() + "_L");
				rl = r.getSchema().getRelation(r.getRelation().getLocalInsDbName());//.getDbRelName() + "_L");
			} catch (RelationNotFoundException rnf) {
				// Skip if there's no _L relation
				Debug.println("Don't create l2p rule because relation " + r.getRelation().getRelationName() + " has no local data");
				continue;
			}

			RelationContext rlc = new RelationContext(rl, r.getSchema(), r.getPeer(), false);

			List<AtomArgument> l = new ArrayList<AtomArgument>();
			List<AtomArgument> hl = new ArrayList<AtomArgument>();

			/*
			for(int k = 0; k < r.getRelation().getFields().size(); k++){
				String v = Mapping.getFreshAutogenVariableName();
				l.add(new AtomVariable(v));
				if(Config.getEdbbits() && k == r.getRelation().getFields().size()-1){
					AtomConst c = new AtomConst("1");
					c.setType(new IntType(false, true));
					hl.add(c);
				}else{
					hl.add(new AtomVariable(v));
				}
			}*/
			for(int k = 0; k < rl.getFields().size(); k++){
				String v = Mapping.getFreshAutogenVariableName();
				l.add(new AtomVariable(v));
				hl.add(new AtomVariable(v));
			}
			if (Config.getEdbbits()){
				AtomConst c = new AtomConst("1");
				c.setType(new IntType(false, true));
				hl.add(c);
			}


			Atom ra = new Atom(r, hl);
			Atom rla = new Atom(rlc, l);
//			List<ScMappingAtom> h = new ArrayList<ScMappingAtom>();
			List<Atom> b = new ArrayList<Atom>();
//			h.add(ra);
//			b.add(rla);

//			ScMapping copyL = new ScMapping("COPY" + r.getRelation().getDbRelName() + "_L",  "COPY" + r.getRelation().getDbRelName() + "_L", true, 1, h, b);

			b.add(rla);
			Rule copyL = new Rule(ra, b, null, builtInSchemas);
			copyL.setFakeMapping(true);
			copyL.setId("BASE" + ra.getRelation().getName());
			ret.add(copyL);
		}
		return ret;
	}
}
