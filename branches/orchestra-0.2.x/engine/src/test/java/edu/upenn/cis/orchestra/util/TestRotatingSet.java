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
public class TestRotatingSet {

	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAdd() {
		RotatingSet<Integer> set = new RotatingSet<Integer>();
		assertTrue(set.add(3));
		assertTrue(set.add(4));
		assertTrue(set.add(5));
		assertFalse(set.add(4));
		assertEquals(Integer.valueOf(3),set.next());
		assertEquals(Integer.valueOf(4),set.next());
		assertEquals(Integer.valueOf(5),set.next());
		assertEquals(Integer.valueOf(3),set.next());
		assertEquals(Integer.valueOf(4),set.next());
		assertEquals(Integer.valueOf(5),set.next());
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testRemove() {
		RotatingSet<Integer> set = new RotatingSet<Integer>();
		assertTrue(set.add(3));
		assertTrue(set.add(4));
		assertTrue(set.add(5));
		assertEquals(Integer.valueOf(3),set.next());
		assertTrue(set.remove(3));
		assertFalse(set.remove(3));
		assertEquals(Integer.valueOf(4),set.next());
		assertEquals(Integer.valueOf(5),set.next());
		assertEquals(Integer.valueOf(4),set.next());
		assertEquals(Integer.valueOf(5),set.next());
		assertTrue(set.remove(4));
		assertFalse(set.remove(4));
		assertEquals(Integer.valueOf(5),set.next());
		assertTrue(set.add(4));
		assertEquals(Integer.valueOf(5),set.next());
		assertEquals(Integer.valueOf(4),set.next());		
	}
}
