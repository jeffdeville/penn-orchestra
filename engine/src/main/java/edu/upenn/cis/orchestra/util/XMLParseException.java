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

import java.util.Map;

import org.w3c.dom.Element;

public class XMLParseException extends Exception {
	private static final long serialVersionUID = 1L;

	public final String node;
	
	public XMLParseException(String msg) {
		this(msg,(Element) null);
	}
	
	public XMLParseException(Throwable cause) {
		super(cause);
		this.node = null;
	}
	
	public XMLParseException(String msg, Throwable cause) {
		super(msg,cause);
		this.node = null;
	}
	
	public XMLParseException(String msg, Element el) {
		super(msg);
		this.node = getString(el);
	}
	
	public XMLParseException(Throwable cause, Element el) {
		super(cause);
		this.node = getString(el);
	}
	
	public XMLParseException(String msg, Throwable cause, Element el) {
		super(msg,cause);
		this.node = getString(el);
	}
	
	private String getString(Element el) {
		if (el == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder("<" + el.getTagName());
		Map<String,String> attr = DomUtils.getAttributes(el);
		for (Map.Entry<String, String> att : attr.entrySet()) {
			sb.append(" " + att.getKey() + "=\"" + att.getValue() + "\"");
		}
		sb.append(">");
		return sb.toString();
		
	}
	
	public String toString() {
		if (node == null) {
			return getMessage();
		} else {
			return getMessage() + ": " + node;
		}
	}
}
