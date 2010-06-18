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
package edu.upenn.cis.orchestra.logicaltypes;

import java.util.Arrays;

public class PrimeNumbers {
	private static final int primes[] = {29, 53, 79, 103, 151, 211, 251, 307, 401, 503, 601, 701, 809, 907, 1009,
		1259, 1511, 1753, 2003, 2251, 2503, 2753, 3001, 3511, 4001, 4507, 5003, 5507, 6007, 6251, 7001, 7507, 8009,
		9001, 10007, 11003, 12007, 13001, 14009, 15013, 16001, 17011, 18013, 19001, 20011, 25013, 30011, 35023,
		40009, 45007, 50021, 55001, 60013, 65003, 70001, 75011, 80021, 85009, 90001, 95003, 100003, 110017,
		120011, 130021, 140009, 150001, 160001, 170003, 180001, 190027, 200003, 225023, 250007, 275003, 300007,
		325001, 350003, 375017, 400009, 425003, 450001, 475037, 500009, 550007, 600011, 700001, 800011, 900001,
		1000003	};

	public static int getNextLargerPrime(int num) {
		if (num >= primes[primes.length -1]) {
			return primes[primes.length -1];
		} else if (num <= primes[0]) {
			return primes[0];
		}
		int index = Arrays.binarySearch(primes, num);
		if (index >= 0) {
			return primes[index];
		} else {
			return primes[-1*(index+1)];
		}
	}

}
