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
package edu.upenn.cis.orchestra.sql;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.TestUtil;

/**
 * Test an {@link ISqlUtil}.
 * 
 * @author Sam Donnelly
 */
@Test(groups = TestUtil.FAST_TESTNG_GROUP)
public class TestSqlUtil {

	ISqlUtil sqlUtil = SqlFactories.getSqlFactory().newSqlUtils();

	/**
	 * Test {@link ISqlUtil#upcaseNonDelimitedText(String, char)}.
	 */
	public void upcaseNonDelimitedText() {
		final String upperCased = sqlUtil.upcaseNonDelimitedText(
				"xXxXx  'xXxXx' xXxXx'xXxXx'", '\'');
		Assert.assertEquals(upperCased, "XXXXX  'xXxXx' XXXXX'xXxXx'");
	}
}
