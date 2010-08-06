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

public class RankSemiring implements Semiring {
	public static RankSemiring ZERO = new RankSemiring("0.0");
	public static RankSemiring ONE = new RankSemiring("1.0");
	
	public RankSemiring(String nameOrValue) {
		baseValue = nameOrValue;
	}
	
	public RankSemiring times(Semiring right) {
		return new RankSemiring("(" + baseValue + ") * (" + right.toString() + ")"); 
	}
	
	public RankSemiring plus(Semiring right) {
		return new RankSemiring("(" + baseValue + ") * (" + right.toString() + ")"); 
	}
	
	public RankSemiring zero() {
		return ZERO;
	}
	
	public RankSemiring one() {
		return ONE;
	}
	
	String baseValue = null;
}
