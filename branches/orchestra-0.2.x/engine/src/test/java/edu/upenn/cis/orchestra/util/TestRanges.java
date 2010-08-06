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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import edu.upenn.cis.orchestra.TestUtil;

@org.testng.annotations.Test(groups = { TestUtil.FAST_TESTNG_GROUP })
public class TestRanges {
	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testConsecutiveRanges() {
		Ranges r = new Ranges();

		for (int i = 0; i < 100; i++) {
			r.add(i);
		}

		List<Ranges.Range> rs = r.getRanges();

		assertEquals("Incorrect range", Collections.singletonList(new Ranges.Range(0,99)), rs);
	}

	@Test
  @org.testng.annotations.Test(groups = JUNIT4_TESTNG_GROUP)
	public void testNonconsecutiveRanges() {
		Ranges r = new Ranges();
		for (int i = 0; i < 100; i++) {
			r.add(i);
		}

		for (int i = 200; i < 300; i++) {
			r.add(i);
		}
		
		List<Ranges.Range> expected = new ArrayList<Ranges.Range>();
		expected.add(new Ranges.Range(0,99));
		expected.add(new Ranges.Range(200,299));

		assertEquals("Incorrect ranges", expected, r.getRanges());
}
}
