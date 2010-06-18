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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Update;

/**
 * A more or less immutable implementation of {@code ILocalUpdates}.
 * @author John Frommeyer
 *
 */
public class LocalUpdates implements ILocalUpdates {
	
	/**
	 * Creates a {@code LocalUpdates} instance.
	 * 
	 * @author John Frommeyer
	 *
	 */
	public static class Builder {

		private final Map<Schema, Map<Relation, List<Update>>> schemaToRelationToUpdates = newHashMap();
		private final Peer peer;
		
		/**
		 * Create a builder for a set of local updates for {@code peer}. 
		 * 
		 * @param peer
		 */
		public Builder(@SuppressWarnings("hiding") final Peer peer) {
			this.peer = peer;
		}
		
		/**
		 * Register {@code update} for {@code schema}.{@code relation}.
		 * 
		 * @param schema
		 * @param relation
		 * @param update
		 */
		public void addUpdate(Schema schema, Relation relation, Update update){
			Map<Relation, List<Update>> relationToUpdates = schemaToRelationToUpdates.get(schema);
			if (relationToUpdates == null) {
				relationToUpdates = newHashMap();
				schemaToRelationToUpdates.put(schema, relationToUpdates);
			}
			List<Update> updates = relationToUpdates.get(relation);
			if (updates == null) {
				updates = newArrayList();
				relationToUpdates.put(relation, updates);
			}
			updates.add(update);
		}
		
		/**
		 * Returns a new {@code ILocalUpdates}.
		 * 
		 * @return a new {@code ILocalUpdates}
		 */
		public ILocalUpdates buildLocalUpdates() {
			return new LocalUpdates(this);
		}

	}

	private final Map<Schema, Map<Relation, List<Update>>> schemaToRelationToUpdates;
	private final Peer peer;
	
	private LocalUpdates(Builder builder) {
		schemaToRelationToUpdates = builder.schemaToRelationToUpdates;
		peer = builder.peer;
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.extractupdates.ILocalUpdates#getLocalUpdates(edu.upenn.cis.orchestra.datamodel.Schema, edu.upenn.cis.orchestra.datamodel.Relation)
	 */
	@Override
	public List<Update> getLocalUpdates(Schema schema, Relation relation) {
		Map<Relation, List<Update>> relationToUpdate = schemaToRelationToUpdates.get(schema);
		if (relationToUpdate == null) {
			return Collections.emptyList();
		}
		List<Update> updates = relationToUpdate.get(relation);
		if (updates == null) {
			return Collections.emptyList();
		}
		return Collections.unmodifiableList(updates);
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.localupdates.ILocalUpdates#getPeer()
	 */
	@Override
	public Peer getPeer() {
		return peer;
	}

}
