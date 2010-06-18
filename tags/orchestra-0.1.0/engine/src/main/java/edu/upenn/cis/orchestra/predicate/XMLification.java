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
package edu.upenn.cis.orchestra.predicate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class XMLification {
	public static final String predTypeAttribute = "predType";
	private static class TagDatum {
		String tagName;
		Class<? extends Predicate> tagClass;
		
		TagDatum(String tagName, Class<? extends Predicate> tagClass) {
			this.tagName = tagName;
			this.tagClass = tagClass;
		}
	}
	
	private static List<TagDatum> tagData = new ArrayList<TagDatum>();
	
	private static String getTag(Predicate p) {
		for (TagDatum td : tagData) {
			if (td.tagClass.isInstance(p)) {
				return td.tagName;
			}
		}
		return null;
	}
	
	private static Class<? extends Predicate> getClass(String tag) {
		for (TagDatum td : tagData) {
			if (td.tagName.equals(tag)) {
				return td.tagClass;
			}
		}
		return null;
	}
	
	private static void registerType(String tagName, Class<? extends Predicate> tagClass) {
		tagData.add(new TagDatum(tagName, tagClass));
	}
	
	static {
		registerType("and", AndPred.class);
		registerType("compare", ComparePredicate.class);
		registerType("not", NotPred.class);
		registerType("or", OrPred.class);
		registerType("equality", EqualityPredicate.class);
	}
	
	public static void serialize(Predicate p, Document d, Element e, AbstractRelation ts) {
		String tag = getTag(p);
		if (tag == null) {
			throw new IllegalArgumentException("Don't have tag registered for predicate of type " + p.getClass().getName());
		}
		e.setAttribute(predTypeAttribute, tag);
		p.serialize(d, e, ts);
	}
	
	public static Predicate deserialize(Element el, AbstractRelation ts) throws XMLParseException {
		String tag = el.getAttribute(predTypeAttribute);
		Class<? extends Predicate> c = getClass(tag);
		if (c == null) {
			throw new XMLParseException("Cannot find class to deserialize predicate", el);
		}
		try {
			Method m = c.getDeclaredMethod("deserialize", Element.class, AbstractRelation.class);
			Object o = m.invoke(null, el, ts);
			return (Predicate) o;
		} catch (NoSuchMethodException e) {
			throw new XMLParseException("Could not find deserialize method for class " + c.getName(), el);
		} catch (IllegalAccessException e) {
			throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), el);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof XMLParseException) {
				throw (XMLParseException) e.getCause();
			} else {
				throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), e.getCause());
			}
		}
		
	}
}
