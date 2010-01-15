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

import org.testng.annotations.BeforeMethod;
import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.TestUtil;

public class TestCache implements Cache.EvictionHandler<Integer,Integer> {
	Integer one, two, three, four;
	LRUCache<Integer,Integer> cache;
	ArrayList<Integer> evictedKeys;
	ArrayList<Integer> evictedValues;

	public void wasEvicted(Integer key, Integer value) {
		evictedKeys.add(key);
		evictedValues.add(value);
	}

	@Before
  @BeforeMethod(groups = { JUNIT4_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
	public void setUp() throws Exception {
		one = 1;
		two = 2;
		three = 3;
		four = 4;
		evictedKeys = new ArrayList<Integer>();
		evictedValues = new ArrayList<Integer>();
	}
	
	@Test
  @org.testng.annotations.Test(groups = {JUNIT4_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
	public void testInsertionAndEviction() throws Exception {
		cache = new EntryBoundLRUCache<Integer,Integer>(2, this);
		cache.store(one, one);
		assertNotNull(cache.probe(one));
		cache.store(two, two);
		assertNotNull(cache.probe(two));
		cache.store(three, three);
		assertNotNull(cache.probe(three));
		assertNull(cache.probe(one));
		assertEquals(1, evictedKeys.size());
		assertEquals(1, evictedValues.size());
		int evictedKey = evictedKeys.get(0);
		int evictedValue = evictedValues.get(0);
		assertEquals(1, evictedKey);
		assertEquals(1, evictedValue);
	}
	
	@Test
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, TestUtil.FAST_TESTNG_GROUP })
	public void testClear() throws Exception {
		cache = new EntryBoundLRUCache<Integer,Integer>(2, this);
		cache.store(one, one);
		assertNotNull(cache.probe(one));
		cache.clear(one);
		assertNull(cache.probe(one));
	}

}
