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
package edu.upenn.cis.orchestra.localupdates.extract.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractorFactory;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.NoExtractorClassException;

/**
 * Creates a {@code IExtractorFactory<java.sql.Connection>} instance based on
 * the value of the {@code localupdates.extractorClass} property.
 * 
 * @author John Frommeyer
 * 
 */
public class ExtractorFactoryJdbc implements IExtractorFactory<Connection> {

	/*
	 * (non-Javadoc)
	 * 
	 * @seeedu.upenn.cis.orchestra.localupdates.extract.IExtractorFactory#
	 * getExtractUpdateInst(edu.upenn.cis.orchestra.datamodel.Peer)
	 */

	@Override
	public IExtractor<Connection> getExtractUpdateInst()
			throws NoExtractorClassException {
		IExtractor<Connection> resCl = null;
		String className = Config.getProperty("localupdates.extractorClass");
		if (className == null) {
			resCl = new ExtractorDefault();
		} else {
			try {
				@SuppressWarnings("unchecked")
				Class<? extends IExtractor<Connection>> extractUpdatesClass = (Class<? extends IExtractor<Connection>>) Class
						.forName(className);
				Constructor<? extends IExtractor<Connection>> constructor = extractUpdatesClass
						.getConstructor();
				resCl = constructor.newInstance();
			} catch (ClassNotFoundException e) {
				throw new NoExtractorClassException(e);
			} catch (ClassCastException e) {
				throw new NoExtractorClassException(
						className + " does not implement IExtractor<Connector>.",
						e);
			} catch (SecurityException e) {
				throw new NoExtractorClassException(e);
			} catch (NoSuchMethodException e) {
				throw new NoExtractorClassException(e);
			} catch (IllegalArgumentException e) {
				throw new NoExtractorClassException(e);
			} catch (InstantiationException e) {
				throw new NoExtractorClassException(e);
			} catch (IllegalAccessException e) {
				throw new NoExtractorClassException(e);
			} catch (InvocationTargetException e) {
				throw new NoExtractorClassException(e);
			}

		}

		// Returned the instance initialized
		return resCl;
	}
}
