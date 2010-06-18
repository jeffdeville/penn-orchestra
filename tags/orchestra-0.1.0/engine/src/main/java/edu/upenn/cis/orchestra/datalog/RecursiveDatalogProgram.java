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
package edu.upenn.cis.orchestra.datalog;

import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * 
 * @author gkarvoun
 *
 */
public class RecursiveDatalogProgram extends DatalogProgram {
	public RecursiveDatalogProgram(List<Rule> r, boolean c4f){
		super(r, c4f);
	}
	
	public RecursiveDatalogProgram(List<Rule> r){
		super(r);
	}

	public String toString ()
	{		
		return "Recursive Datalog Program { \n" + super.toString() + "} END Recursive Datalog Program\n";
	}

	@Override
	public Element serialize(Document document) {
		Element e = super.serialize(document);
		e.setAttribute("programType", "recursiveDatalogProgram");
		return e;
	}

}
