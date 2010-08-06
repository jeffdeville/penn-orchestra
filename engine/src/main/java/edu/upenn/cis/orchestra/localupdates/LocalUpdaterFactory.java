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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.localupdates.apply.sql.IDerivabilityCheck;
import edu.upenn.cis.orchestra.localupdates.exceptions.NoLocalUpdaterClassException;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.NoExtractorClassException;
import edu.upenn.cis.orchestra.localupdates.sql.LocalUpdaterJdbc;

/**
 * A factory for obtaining {@code ILocalUpdater} instances.
 * 
 * @author John Frommeyer
 * 
 */
public class LocalUpdaterFactory {

	/**
	 * Returns a new {@code ILocalUpdater} based on the value of the {@code
	 * localupdates.localUpdaterClass } property and which is capable of
	 * connecting to {@code server} as {@code user} with the password {@code
	 * password}.
	 * 
	 * @param user
	 * @param password
	 * @param server
	 * @param derivabilityChecker 
	 * 
	 * @return a new {@code ILocalUpdater}
	 * @throws NoLocalUpdaterClassException if the value of the {@code
	 *             localupdates.localUpdaterClass } does not implement {@code
	 *             ILocalUpdater}
	 * @throws NoExtractorClassException
	 */
	public static ILocalUpdater newInstance(String user, String password,
			String server, IDerivabilityCheck derivabilityChecker)
			throws NoLocalUpdaterClassException, NoExtractorClassException {
		String className = Config.getProperty("localupdates.localUpdaterClass");
		ILocalUpdater updater = null;
		if (className == null) {
			updater = new LocalUpdaterJdbc(user, password, server, derivabilityChecker);
		} else {
			try {
				@SuppressWarnings("unchecked")
				Class<? extends ILocalUpdater> localUpdaterClass = (Class<? extends ILocalUpdater>) Class
						.forName(className);
				Constructor<? extends ILocalUpdater> constructor = localUpdaterClass
						.getConstructor();
				updater = constructor.newInstance();
			} catch (ClassNotFoundException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (ClassCastException e) {
				throw new NoLocalUpdaterClassException(className
						+ " does not implement ILocalUpdater.", e);
			} catch (SecurityException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (NoSuchMethodException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (IllegalArgumentException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (InstantiationException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (IllegalAccessException e) {
				throw new NoLocalUpdaterClassException(e);
			} catch (InvocationTargetException e) {
				throw new NoLocalUpdaterClassException(e);
			}
		}
		return updater;
	}
}
