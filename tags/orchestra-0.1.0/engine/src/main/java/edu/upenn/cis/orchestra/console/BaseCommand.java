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
package edu.upenn.cis.orchestra.console;

import java.util.Map;

public abstract class BaseCommand implements ConsoleCommand {
	protected String m_name;
	protected String m_params;
	protected String m_help;
	protected ParameterParser m_parser;
	
	public BaseCommand(String name, String params, String help) {
		m_name = name;
		m_params = params;
		m_help = help;
		m_parser = new ParameterParser(params);
	}
	
	public String name() { 
		return m_name; 
	}

	public String params() {
		return m_params;
	}

	public String help() {
		return m_help;
	}
	
	public String usage() {
		return m_name + "\t" + m_params + "\t" + m_help;
	}
	
	public void execute(String[] args) throws CommandException {
		StringBuffer buf = new StringBuffer();
		for (int i = 1; i < args.length; i++) {
			buf.append(args[i]);
			buf.append(' ');
		}
		Map<String,String> params = m_parser.parse(buf.toString());
		if (params == null) {
			throw new CommandException(shortUsage());
		} else {
			myExecute(params);
		}
	}
	
	protected String shortUsage() {
		return "Usage: " + m_name + " " + m_params;
	}
	
	protected abstract void myExecute(Map<String,String> args) throws CommandException;
}
