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

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;


/**
 * A null description should not cause {@toString()} to throw a {@code
 * NullPointerException}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class SchemaToStringTest {
	/**
	 * Passes if no exceptions are thrown. We had a bug where Schemas with no
	 * descriptions were throwing null pointer exceptions when toString() was
	 * called.
	 * 
	 */
	public void testToString() {
		Schema s = new Schema(getClass().getSimpleName() + "_schema");
		try {
			s.toString();
		} catch (NullPointerException e) {
			fail();
		}
	}
}
