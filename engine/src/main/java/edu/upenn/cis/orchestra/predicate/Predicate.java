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
package edu.upenn.cis.orchestra.predicate;

import java.io.Serializable;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.logicaltypes.Filter;

/**
 * Define how we can interact with a tuple-level predicate.
 * 
 * @author Nick Taylor
 */
public interface Predicate extends Serializable, Filter<AbstractTuple<?>> {
	/**
	 * Evaluate the predicate over the supplied tuple.
	 * 
	 * @param t				The tuple to evaluate the predicate over
	 * @return				<code>true</code> if the predicate is satisfied,
	 * 						<code>false</code> if it is not
	 * @throws CompareMismatch
	 * 						If a type error occurs
	 */
	public abstract boolean eval(AbstractTuple<?> t) throws CompareMismatch;
	
	/**
	 * Returns a string encoding this predicate in SQL. The specified prefix
	 * and suffix are inserted into the predicate before and after any column
	 * names.
	 * 
	 * @param prefix		Prefix for column names
	 * @param suffix		Suffix for column names
	 * @return				The predicate in SQL
	 */
	public abstract String getSqlCondition(AbstractRelation ts, String prefix, String suffix);
	
	/**
	 * Serialize a predicate to an element in an XML document.
	 * 
	 * @param d			The document
	 * @param el		The element in the document
	 * @param ts		The schema of the relation to which the predicate
	 * 					refers
	 */
	public abstract void serialize(Document d, Element el, AbstractRelation ts);	
}