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
public class TestLongSet {
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAdd() {
		LongSet ls = new LongSet(3);
		assertTrue(ls.add(1));
		assertTrue(ls.add(2));
		assertTrue(ls.add(4));
		assertFalse(ls.add(4));
		
		assertTrue(ls.contains(1));
		assertTrue(ls.contains(2));
		assertTrue(ls.contains(1));
		assertEquals(3, ls.size());
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testRemove() {
		LongSet ls = new LongSet(3);
		assertTrue(ls.add(1));
		assertTrue(ls.add(4));
		assertTrue(ls.remove(1));
		assertTrue(ls.contains(4));
		assertEquals(1,ls.size());
		assertTrue(ls.add(1));
		assertTrue(ls.contains(1));
		assertTrue(ls.contains(4));
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testRemoveFrom() {
		LongSet ls = new LongSet(17);
		assertTrue(ls.add(1));
		assertTrue(ls.add(4));
		assertTrue(ls.add(15));
		assertTrue(ls.add(18));
		assertTrue(ls.add(92));

		LongSet ls2 = new LongSet(19);
		assertTrue(ls2.add(1));
		assertTrue(ls2.add(20));
		assertTrue(ls2.add(18));
		
		assertFalse(ls.equals(ls2));
		
		ls.removeAll(ls2);
		assertEquals(3,ls.size());
		assertTrue(ls.contains(4));
		assertTrue(ls.contains(15));
		assertTrue(ls.contains(92));
	}
}
