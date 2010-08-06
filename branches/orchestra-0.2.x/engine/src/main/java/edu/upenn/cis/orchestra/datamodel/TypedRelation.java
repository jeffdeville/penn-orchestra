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
package edu.upenn.cis.orchestra.datamodel;

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;

public class TypedRelation {
	public Relation rel;
	public AtomType typ;

	public TypedRelation(Relation r, AtomType t) {
		rel = r;
		typ = t;
	}

	public boolean equals(TypedRelation other){
		return (rel.equals(other.rel) && typ.equals(other.typ));
	}

	@Override
	public boolean equals(Object o){
		if(o instanceof TypedRelation) {
			return equals((TypedRelation)o);
		}else{
			return false;
		}
	}

	@Override
	public int hashCode(){
		int code = 17;
		code = 31 * code + rel.hashCode();
		code = 31 * code + typ.hashCode();
		return code;//rel.getName().hashCode();
	}
}


