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

import java.util.Collection;



public class TupleSet extends AbstractTupleSet<Relation,Tuple> {
	private static final long serialVersionUID = 1L;

	public TupleSet() {
	}

	public TupleSet(int initialSize) {
		super(initialSize);
	}

	public TupleSet(Collection<? extends Tuple> tuples) {
		super(tuples);
	}
	
	@Override
	protected Class<Relation> getSchemaClass() {
		return Relation.class;
	}
}
