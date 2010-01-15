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
package edu.upenn.cis.orchestra.localupdates;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Update;

/**
 * 
 * Represents a set of local updates to a peer which Orchestra needs to be
 * made aware of.
 * 
 * @author John Frommeyer
 * 
 */
public interface ILocalUpdates {

	/**
	 * Returns a list of updates which have been applied to {@code schema}.
	 * {@code relation} since the last publish operation.
	 * 
	 * @param schema
	 * @param relation
	 * @return the updates for the given {@code Schema} and {@code Relation}
	 */
	List<Update> getLocalUpdates(Schema schema, Relation relation);
	
	/**
	 * Returns the peer these updates belong to.
	 * 
	 * @return the peer to which these updates belong
	 */
	Peer getPeer();
	
}
