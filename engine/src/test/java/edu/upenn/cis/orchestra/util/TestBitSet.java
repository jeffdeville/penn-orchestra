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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import edu.upenn.cis.orchestra.TestUtil;

@org.testng.annotations.Test(groups = { TestUtil.FAST_TESTNG_GROUP })
public class TestBitSet {
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testCreate() {
		BitSet bs = new BitSet(8);
		assertEquals(1, bs.getData().length);
		bs = new BitSet(9);
		assertEquals(2, bs.getData().length);
		bs = new BitSet(15);
		assertEquals(2, bs.getData().length);
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAllZeros() {
		BitSet bs = new BitSet(15);
		for (int i = 0; i < 15; ++i) {
			assertFalse(bs.get(i));
		}
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAllOnes() {
		BitSet bs = new BitSet(15);
		for (int i = 0; i < 15; ++i) {
			bs.set(i);
		}		
		for (int i = 0; i < 15; ++i) {
			assertTrue(bs.get(i));
		}
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testSome() {
		BitSet bs = new BitSet(32);
		Set<Integer> set = new HashSet<Integer>();
		int[] vals = {3, 7, 8, 19, 25, 31};
		for (int val : vals) {
			set.add(val);
			bs.set(val);
		}
		Set<Integer> found = new HashSet<Integer>();
		for (int i = 0; i < 32; ++i) {
			if (bs.get(i)) {
				found.add(i);
			}
		}
		assertEquals(set,found);
	}
}
