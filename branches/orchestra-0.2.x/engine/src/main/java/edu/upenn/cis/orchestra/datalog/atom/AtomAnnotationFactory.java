package edu.upenn.cis.orchestra.datalog.atom;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * Factory for constructing semiring annotations (trust, transaction, etc.) and
 * the means of evaluating them
 * 
 * @author zives
 *
 */
public class AtomAnnotationFactory {
	/**
	 * The trust attribute -- which can be rank or integer-based (Boolean is a special
	 * case of Integer)
	 * 
	 * @param peerName
	 * @return
	 */
	public static AtomAnnotation createPeerTrustAnnotation(String peerName, String var) {
		if (Config.useIntegerTrust())
			return new AtomAnnotation(peerName + RelationField.ANN_EXT, var, AtomAnnotation.SEMIRING.MINTRUST);
		else
			return new AtomAnnotation(peerName + RelationField.ANN_EXT, var, AtomAnnotation.SEMIRING.RANKS);
	}
	
	/**
	 * The transaction ID is a longint <recno, xid>
	 * 
	 * @return
	 */
	public static AtomAnnotation createTransactionAnnotation(String var) {
		return new AtomAnnotation("XID" + RelationField.ANN_EXT, var, AtomAnnotation.SEMIRING.TRANSACTION);
	}
}
