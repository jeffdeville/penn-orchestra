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
package edu.upenn.cis.orchestra;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Project-wide utility class.
 * 
 * @author Sam Donnelly
 */
public class OrchestraUtil {

	/**
	 * Convenience {@code ArrayList} maker.
	 * <p>
	 * So we can write
	 * <p>
	 * {@code List<Integer> integers = newArrayList()}
	 * <p>
	 * instead of
	 * <p>
	 * {@code List<Integer> integers = new ArrayList<Integer>()}.
	 * 
	 * @param <T> the parameter of {@code new ArrayList<T>()}
	 * @return the new {@code ArrayList<T>}
	 */
	public static <T> ArrayList<T> newArrayList() {
		return new ArrayList<T>();
	}

	/**
	 * Convenience {@code ArrayList} maker.
	 * <p>
	 * So we can write
	 * <p>
	 * {@code List<Integer> integers = newArrayList(c)}
	 * <p>
	 * instead of
	 * <p>
	 * {@code List<Integer> integers = new ArrayList<Integer>(c)}.
	 * 
	 * @param <T> the parameter of {@code new ArrayList<T>()}
	 * @param c the collection whose elements are to be placed into this list
	 * @return the new {@code ArrayList<T>}
	 */
	public static <T> ArrayList<T> newArrayList(final Collection<? extends T> c) {
		return new ArrayList<T>(c);
	}

	/**
	 * Convenience {@link ArrayList} maker.
	 * <p>
	 * So we can write
	 * <p>
	 * {@code List<Integer> integers = newArrayList(4, 5, 3)}
	 * <p>
	 * instead of
	 * <p>
	 * {@code List<Integer> integers = new ArrayList<Integer>(Arrays.asList(4,
	 * 5, 3))}.
	 * 
	 * @param <T> the parameter of {@code new ArrayList<T>()}.
	 * @param objects objects that should go into the returned {@code ArrayList}
	 *            .
	 * @return the new {@code ArrayList<T>}
	 */
	public static <T> ArrayList<T> newArrayList(T... objects) {
		return new ArrayList<T>(Arrays.asList(objects));
	}

	/**
	 * Shorthand for {@code new Vector<T>()}.
	 * 
	 * @param <T> the parameter of {@code new Vector<T>()}
	 * @return {@code new Vector<T>()}
	 */
	public static <T> Vector<T> newVector() {
		return new Vector<T>();
	}

	/**
	 * Shorthand for {@code new Vector<T>(c)}.
	 * 
	 * @param <T> see description
	 * @param c see description
	 * @return {@code new Vector<T>(c)}.
	 */
	public static <T> Vector<T> newVector(final Collection<? extends T> c) {
		return new Vector<T>(c);
	}

	/**
	 * Convenience {@code HashSet} creator equivalent to {@code new
	 * HashSet<T>()}.
	 * 
	 * @param <T> see description
	 * @return {@code new HashSet<T>()}
	 */
	public static <T> HashSet<T> newHashSet() {
		return new HashSet<T>();
	}

	/**
	 * Convenience {@code HashSet} creator equivalent to {@code new
	 * HashSet<T>(c)}.
	 * 
	 * @param <T> see description
	 * @param c
	 * @return {@code new HashSet<T>(c)}
	 */
	public static <T> HashSet<T> newHashSet(final Collection<? extends T> c) {
		return new HashSet<T>(c);
	}

	/**
	 * Convenience {@code HashMap} creator equivalent to {@code new HashMap<K,
	 * V>()}.
	 * 
	 * @param <K> see description
	 * @param <V> see description
	 * @return {@code new HashMap<K, V>()}.
	 */
	public static <K, V> HashMap<K, V> newHashMap() {
		return new HashMap<K, V>();
	}

	/**
	 * Convenience {@code TreeMap} creator equivalent to {@code new TreeMap<K,
	 * V>()}.
	 * 
	 * @param <K> see description
	 * @param <V> see description
	 * @return {@code new TreeMap<K, V>()}.
	 */
	public static <K, V> TreeMap<K, V> newTreeMap() {
		return new TreeMap<K, V>();
	}

	/**
	 * Returns the directory part of the path to the Orchestra schema file
	 * indicated by {@code schemaName}. The Orchestra schema file should be in a
	 * subdirectory of {@code clazz}'s package called {@code schemaName}.
	 * 
	 * @param schemaName the name of the Orchestra schema (without the '.schema'
	 *            extension)
	 * @param clazz the {@code Class} in whose package the schema is located
	 * 
	 * @return the working directory for the schema {@code schemaFile}
	 */
	public static String getWorkingDirectory(String schemaName, Class<?> clazz) {
		String schemaFilename = schemaName + ".schema";
		URL schemaURL = clazz.getResource(schemaName + "/" + schemaFilename);

		if (schemaURL == null) {
			String packageName = clazz.getPackage().getName();
			String altName = packageName + "/" + schemaFilename;
			throw new IllegalArgumentException("Could not find "
					+ schemaFilename + " in " + packageName + " or in "
					+ altName + ".");
		}
		String workingDirectory = schemaURL.getPath();
		int dirPathLength = workingDirectory.length() - schemaFilename.length();
		return workingDirectory.substring(0, dirPathLength);
	}

	/**
	 * Returns a {@code Serializable} version of {@code set}.
	 * 
	 * @param <T>
	 * @param set
	 * @return a {@code Serializable} version of {@code set}.
	 */
	public static <T> Set<T> toSerializableSet(Set<T> set) {
		if (set instanceof Serializable) {
			return set;
		}
		return newHashSet(set);
	}

	/** Prevent inheritance and instantiation. */
	private OrchestraUtil() {}

}
