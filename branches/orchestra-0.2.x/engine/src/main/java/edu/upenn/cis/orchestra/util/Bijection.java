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
package edu.upenn.cis.orchestra.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Bijection<T, U> {
	private final Map<T,U> probeT;
	private final Map<U,T> probeU;
	
	private boolean finished = false;
	
	public Bijection() {
		probeT = new HashMap<T,U>();
		probeU = new HashMap<U,T>();
	}
	
	public Bijection(int size) {
		probeT = new HashMap<T,U>(size);
		probeU = new HashMap<U,T>(size);
	}
	
	public void setMapping(T t, U u) {
		if (finished) {
			throw new IllegalStateException("Bijection in read only");
		}
		if (probeT.containsKey(t) || probeU.containsKey(u)) {
			throw new IllegalArgumentException("Mapping already contains one of the supplied values");
		}
		probeT.put(t, u);
		probeU.put(u, t);
	}
	
	public void setFinished() {
		finished = true;
	}
	
	public boolean isFinished() {
		return finished;
	}
	
	public U probeFirst(Object t) {
		return probeT.get(t);
	}
	
	public T probeSecond(Object u) {
		return probeU.get(u);
	}
	
	public Set<T> getFirsts() {
		return Collections.unmodifiableSet(probeT.keySet());
	}
	
	public Set<U> getSeconds() {
		return Collections.unmodifiableSet(probeU.keySet());
	}
	
	public Map<T,U> getFirstMap() {
		return Collections.unmodifiableMap(probeT);
	}
	
	public Map<U,T> getSecondMap() {
		return Collections.unmodifiableMap(probeU);
	}
	
	public String toString() {
		return probeT.toString();
	}
}
