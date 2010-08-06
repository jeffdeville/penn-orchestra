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
package edu.upenn.cis.orchestra;
/**
 * This class contains functions that control the output of debug messages.
 * By default, such messages are printed but they can also be disabled.
 * 
 * @author Grigoris Karvounarakis
 */
public class Debug 
{
	protected static boolean s_midline = false;

	protected static String debugTag() {
		if (Config.getFullDebug() && !s_midline) {
			StackTraceElement[] stack = Thread.getAllStackTraces().get(Thread.currentThread());
			StackTraceElement caller = stack[4];
			String fullclass = caller.getClassName();
			int index = fullclass.lastIndexOf(".");
			String clazz = fullclass.substring(index+1);
			return clazz + "." + caller.getMethodName() + "@" + caller.getLineNumber() + ": ";
		} else {
			return "";
		}
	}
	
	/**
	 * Print a given message if in debug mode.
	 */
	public static void print(String msg) {
		if (Config.getDebug()) {
			//System.err.print(debugTag() + msg);
			System.out.print(debugTag() + msg);
			s_midline = !msg.endsWith("\n");
		}
	}
	/**
	 * Print a given message if in debug mode.
	 */
	public static void println(String msg) {
		if (Config.getDebug()) {
			//System.err.println(debugTag() + msg);
			System.out.println(debugTag() + msg);
			s_midline = false;
		}
	}
}
