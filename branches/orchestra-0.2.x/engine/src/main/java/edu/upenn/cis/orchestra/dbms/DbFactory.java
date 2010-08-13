package edu.upenn.cis.orchestra.dbms;

import java.util.Map;

import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Simple factory for deserializing instances of IDb, the DBMS container
 * for Orchestra
 * 
 * @author zives
 *
 */
public class DbFactory {
	public static IDb deserializeDb(OrchestraSystem catalog, 
			Map<String, Schema> builtInSchemas, Element el) 
	throws XMLParseException {
		String type = el.getAttribute("type");
		if (type.compareToIgnoreCase("sql") == 0) {
			return SqlDb.deserialize(catalog, builtInSchemas, el);
		//} else if (type.compareToIgnoreCase("tukwila") == 0) {
		//	return TukwilaDb.deserialize(catalog, el);
		} else {
			throw new XMLParseException("Unknown database type: " + type, el);
		}
	}
}
