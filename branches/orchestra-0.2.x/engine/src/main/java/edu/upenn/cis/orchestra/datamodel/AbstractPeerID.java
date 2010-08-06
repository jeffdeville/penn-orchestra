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

import java.lang.reflect.Method;
import java.util.HashMap;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Generic class to encapsulate peer IDs.
 * 
 * @author zives and netaylor
 *
 */

public abstract class AbstractPeerID implements Comparable<AbstractPeerID>, java.io.Serializable {
	private static final long serialVersionUID = 1L;
	// Store a mapping from type ID to class object
	private static HashMap<Byte,Class<? extends AbstractPeerID>> getClassForId = new HashMap<Byte,Class<? extends AbstractPeerID>>();
	private static HashMap<Class<? extends AbstractPeerID>,Byte> getIdForClass = new HashMap<Class<? extends AbstractPeerID>,Byte>();

	// Store a mapping from type name to class object
	private static HashMap<String,Class<? extends AbstractPeerID>> getClassForName = new HashMap<String,Class<? extends AbstractPeerID>>();
	private static HashMap<Class<? extends AbstractPeerID>,String> getNameForClass = new HashMap<Class<? extends AbstractPeerID>,String>();

	/**
	 * Subclasses must call this at some point with a unique ID, and implement
	 * static methods
	 * 
	 * PeerID fromSubclassBytes(byte[] bytes, int offset, int length)
	 * PeerID fromStringRep(String rep)
	 * 
	 * @param id		The ID to assign to the subclass
	 * @param name		The name to assign the class
	 * @param c			The class object belonging to the subclass 
	 */
	private static void registerType(byte id, String name, Class<? extends AbstractPeerID> c) {
		getClassForId.put(id,c);
		getIdForClass.put(c,id);
		getClassForName.put(name, c);
		getNameForClass.put(c, name);
	}
	
	static {
		registerType(IntPeerID.typeId, IntPeerID.typeName, IntPeerID.class);
		registerType(StringPeerID.typeId, StringPeerID.typeName, StringPeerID.class);
	}

	/**
	 * Create a new copy of this PeerID
	 * 
	 * @return The copy
	 */
	abstract public AbstractPeerID duplicate(); 

	/**
	 * Create a hash code from the value of this <code>PeerID</code>.
	 * 
	 * It is important that an implementation of <code>PeerID</code> provide
	 * an implementation of this function that works on the value of the object
	 * instead of its address (like the default one from <code>Object</code>).
	 * 
	 * @return The hash code
	 */
	abstract public int hashCode();

	/**
	 * Determine if this PeerID refers to the same peer in its database
	 * as <code>o</code> does
	 * 
	 * @param o		The PeerID to compare to
	 * @return		<code>true</code> if they do refer to the same peer,
	 * 				<code>false</code> if they do not
	 */
	abstract public boolean equals(Object o);

	abstract public int compareTo(AbstractPeerID p);

	/**
	 * Get the ID as a byte string
	 * 
	 * @return An array of bytes that encodes the subclass data for this ID
	 */
	abstract protected byte[] getSubclassBytes();

	abstract protected String getSubclassString();

	/**
	 * Get a byte string that includes a byte identifying the class of the ID
	 * 
	 * @return		The byte array
	 */
	final public byte[] getBytes() {
		byte[] subclassBytes = getSubclassBytes();
		byte id = getIdForClass.get(this.getClass());
		byte[] retval = new byte[subclassBytes.length+1];
		retval[0] = id;
		for (int i = 0; i < subclassBytes.length; ++i) {
			retval[i+1] = subclassBytes[i];
		}

		return retval;
	}

	/**
	 * Decode a byte string representing a PeerID object. The class of the
	 * object is given by the byte included with the byte string
	 * 
	 * @param bytes				The byte array to read from
	 * @param offset			The index of where to start reading
	 * @param length			The amount of data to read
	 * @return					The PeerID object
	 */
	public static AbstractPeerID fromBytes(byte[] bytes, int offset, int length) {
		if (length < 1) {
			throw new RuntimeException("Length must be at least one to decode PeerID object");
		}
		Class<? extends AbstractPeerID> classObj = getClassForId.get(bytes[offset]);
		if (classObj == null) {
			throw new RuntimeException("Class object for PeerID subclass with ID " + bytes[offset] + " not found");
		}
		try {
			Method m = classObj.getDeclaredMethod("fromSubclassBytes", byte[].class, int.class, int.class);
			// Since fromSubclassBytes is a static method, we don't need an object to
			// invoke it on.
			Object retval = m.invoke(null, bytes, offset + 1, length - 1);
			return (AbstractPeerID) retval;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 * Decode a byte string representing a PeerID object. The class of the
	 * object is given by the byte included with the byte string
	 * 
	 * @param bytes				The byte representation of the PeerID object
	 * @return					The PeerID object
	 */
	public static AbstractPeerID fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	protected static byte getIdForClass(Class<? extends AbstractPeerID> c) {
		Byte b = getIdForClass.get(c);
		if (b != null) {
			return b;
		} else {
			throw new RuntimeException("Could not find ID for class " + c.getName());
		}
	}

	public void serialize(Document doc, Element pid) {
		pid.setAttribute("pid", getSubclassString());
		pid.setAttribute("pidType", getNameForClass.get(this.getClass()));
	}

	public static AbstractPeerID deserialize(Element pid) throws XMLParseException {
		String typeName = pid.getAttribute("pidType");
		String rep = pid.getAttribute("pid");
		if (typeName == null) {
			throw new XMLParseException("Element does not contain a 'pidType' attribute", pid);
		}
		if (rep == null) {
			throw new XMLParseException("Element does not contain a 'pid' attribute", pid);
		}

		try {
			return fromStringRepWithType(typeName,rep);
		} catch (Exception e) {
			throw new XMLParseException("Error decoding string representation of PeerID", e);
		}
	}
	
	public static class PeerIDFormatException extends Exception {
		private static final long serialVersionUID = 1L;

		PeerIDFormatException(String what) {
			super(what);
		}
		
		PeerIDFormatException(Throwable what) {
			super(what);
		}
	}
	
	/**
	 * Get a serialization of this peerID to a human- and machine-readable string
	 * 
	 * @return			The serialization
	 */
	public String serialize() {
		return getNameForClass.get(this.getClass()) + ':' + getSubclassString();
	}
	
	/**
	 * 
	 * 
	 * @param typeAndRep			The serialization of a peer ID
	 * @return						The deserialized peer ID
	 * @throws PeerIDFormatException
	 */
	public static AbstractPeerID deserialize(String typeAndRep) throws PeerIDFormatException {
		int index = typeAndRep.indexOf(':');
		if (index == -1) {
			throw new PeerIDFormatException("Serialized peer ID must contain a ':' to seperate the type identifier from the serialized data");
		}
		String type = typeAndRep.substring(0, index);
		String rep = typeAndRep.substring(index + 1);
		
		try {
			return fromStringRepWithType(type, rep);
		} catch (Exception e) {
			throw new PeerIDFormatException(e);
		}
	}
	
	private static AbstractPeerID fromStringRepWithType(String type, String rep) throws Exception {
		Class<? extends AbstractPeerID> classObj = getClassForName.get(type);
		if (classObj == null) {
			throw new Exception("Class object for PeerID subclass with pidType " + type + " not found");
		}
		Method m = classObj.getDeclaredMethod("fromStringRep", String.class);
		// Since fromStringRep is a static method, we don't need an object to
		// invoke it on.
		Object retval = m.invoke(null, rep);
		return (AbstractPeerID) retval;
	}
}
