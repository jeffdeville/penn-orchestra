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
package edu.upenn.cis.orchestra.workload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class RandomizedSet {
	static private final int INITIAL_WEIGHT_LENGTH = 10;
	private static class StringCount {
		private String value;
		private int count;
		private StringCount(String value) {
			this.value = value;
			this.count = 0;
		}
		String getValue() {
			++count;
			return value;
		}
	}
	private ArrayList<StringCount> elements = new ArrayList<StringCount>();
	Random generator;
	
	
	// If the randomly chosen number is in (offsets[i-1],offsets[i]],
	// then elements[i] is the randomly chosen element. offsets[i-1] implicity
	// equals -1.
	private int[] offsets;
	
	public RandomizedSet(boolean weighted) throws IOException {
		this(weighted, new Random());
	}
	
	public RandomizedSet(boolean weighted, Random generator) throws IOException {
		this.generator = generator;
		if (weighted) {
			offsets = new int[INITIAL_WEIGHT_LENGTH];
		}
	}
	
	public void addElement(String el) {
		if (offsets != null) {
			throw new IllegalStateException("Cannot add element without weight to weighted array");
		}
		elements.add(new StringCount(el));
	}
	
	public void addElementWithWeight(String el, int weight) {
		if (offsets == null) {
			throw new IllegalStateException("Cannot add element with weight to non-weighted set");
		}
		elements.add(new StringCount(el));
		final int size = elements.size();
		if (offsets.length < size) {
			int[] newOffsets = new int[offsets.length * 2];
			System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);
			offsets = newOffsets;
		}
		if (size == 1) {
			offsets[0] = weight;
		} else {
			offsets[size-1] = offsets[size-2] + weight;
		}
	}
	
	/**
	 * Choose an element from the set based on the
	 * weights in offsets, if they are defined, otherwise
	 * totally randomly
	 * 
	 * @return
	 */
	public String chooseElement() {
		if (offsets == null) {
			return elements.get(generator.nextInt(elements.size())).getValue(); 
		}
		int num = elements.size();
		int last = offsets[num-1];
		int index = generator.nextInt(last+1);
		
		int lowerBound = 0;
		int upperBound = num - 1;
		int pos;
		for ( ; ; ) {
			pos = (lowerBound + upperBound) / 2;
			int currEnd = offsets[pos];
			int prevEnd = (pos == 0 ? -1 : offsets[pos-1]);
			if (prevEnd < index && index <= currEnd)
				break;
			
			if (currEnd < index) {
				lowerBound = pos + 1;
			} else if (prevEnd >= index) {
				upperBound = pos - 1;
			}
		}
		
		return elements.get(pos).getValue();
	}
	
	/**
	 * Makes a set (previously weighted or unweighted) a Zipfian-weighted
	 * set with the specified exponent.
	 * 
	 * @param s		The Zipfian exponent
	 */
	public void makeWeightedZipfian(double s) {
		int sum = 0;
		double total = 0;
		int numEl = elements.size();
		offsets = new int[numEl];
		
		double exponent = -1.0 * s;
		
		for (int i = 1; i <= numEl; ++i) {
			total += Math.pow(i, exponent);
		}
		
		double multiplier = Integer.MAX_VALUE / total;
		
		for (int i = 1; i <= numEl; ++i) {
			int bucketSize = (int) (Math.pow(i, exponent) * multiplier);	
			sum += bucketSize;
			offsets[i-1] = sum;
		}
	}

	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		final int numEls = elements.size();

		for (int i = 0; i < numEls; ++i) {
			sb.append(elements.get(i).value + (offsets == null ? "\t" : ("\t" + offsets[i])) + "\t" + elements.get(i).count + "\n"); 
		}
		
		return sb.toString();
	}
	
}
