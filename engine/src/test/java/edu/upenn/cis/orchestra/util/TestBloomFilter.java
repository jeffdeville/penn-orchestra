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


import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static org.testng.AssertJUnit.*;

import org.junit.Test;

import edu.upenn.cis.orchestra.TestUtil;

@org.testng.annotations.Test(groups = { TestUtil.FAST_TESTNG_GROUP })
public class TestBloomFilter {
	private final BloomFilter.Hasher<Integer> hasher = new BloomFilter.Hasher<Integer>() {
		@Override
		public int hashCode1(Integer val) {
			return val;
		}

		@Override
		public int hashCode2(Integer val) {
			return 37 * val + 1;
		}
	};
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testInsertion() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		bf.add(17);
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testContains() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		assertFalse(bf.contains(17));
		bf.add(17);
		assertTrue(bf.contains(17));
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testDoesNotContain() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		bf.add(17);
		bf.add(91);
		bf.add(999);
		assertFalse(bf.contains(19));
	}
}
