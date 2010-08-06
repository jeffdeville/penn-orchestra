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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.IntType;
import static org.testng.AssertJUnit.*;

@org.testng.annotations.Test(groups = { TestUtil.FAST_TESTNG_GROUP })
public class TestByteArraySet {
	byte[] one = IntType.getBytes(1), two = IntType.getBytes(2), three = IntType.getBytes(3);
	
	ByteArraySet.Deserializer<Integer> d = new ByteArraySet.Deserializer<Integer>() {

		@Override
		public Integer fromBytes(byte[] data, int offset, int length) {
			return IntType.getValFromBytes(data);
		}
		
	};
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testEmpty() {
		ByteArraySet bas = new ByteArraySet(10);
		assertEquals(0, bas.size());
	}

	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testAdd() {
		ByteArraySet bas = new ByteArraySet(10);
		assertTrue(bas.add(one));
		assertFalse(bas.add(one));
		assertTrue(bas.add(two));
		assertFalse(bas.add(two));
		assertEquals(2,bas.size());
		bas.clear();
		assertFalse(bas.remove(one));
		assertFalse(bas.remove(two));
		assertEquals(0,bas.size());
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testRemove() {
		ByteArraySet bas = new ByteArraySet(10);
		assertTrue(bas.add(one));
		assertTrue(bas.add(two));
		assertTrue(bas.remove(one));
		assertEquals(1, bas.size());
		assertTrue(bas.remove(two));
		assertFalse(bas.remove(two));
		assertFalse(bas.remove(one));
		assertEquals(0,bas.size());
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testCollisions() {
		ByteArraySet bas = new ByteArraySet(10);
		int numBuckets = bas.numBuckets();
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertTrue(bas.add(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertFalse(bas.add(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertTrue(bas.remove(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertFalse(bas.remove(IntType.getBytes(i)));
		}
	}
	
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testIterator() {
		Set<Integer> expected = new HashSet<Integer>();
		ByteArraySet bas = new ByteArraySet(10);
		int numBuckets = bas.numBuckets();
		int max = 5 * numBuckets;
		for (int i = 0; i < max; ++i) {
			expected.add(i);
		}
		for (int i = 0; i < max; ++i) {
			assertTrue(bas.add(IntType.getBytes(i)));
		}
		Set<Integer> found = new HashSet<Integer>();
		Iterator<Integer> it = bas.iterator(d);
		while (it.hasNext()) {
			found.add(it.next());
		}
		assertEquals(expected, found);
	}
}
