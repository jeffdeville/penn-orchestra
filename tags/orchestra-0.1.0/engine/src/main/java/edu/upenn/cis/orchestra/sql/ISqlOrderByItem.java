package edu.upenn.cis.orchestra.sql;

/**
 * An item which can be part of an {@code ORDER BY} clause.
 * @author John Frommeyer
 *
 */
public interface ISqlOrderByItem extends ISqlExp {
	/**
	 * How to order the results
	 * 
	 */
	public enum OrderType {
		
		/** Ascending */
		ASC, 
		/** Descending */
		DESC,
		/** None */
		NONE;
	}
	
	/**
	 * How nulls are ordered.
	 *
	 */
	public enum NullOrderType {
		
		/** Nulls first */
		NULLS_FIRST, 
		/** Nulls last */
		NULLS_LAST, 
		/** None */
		NONE;
	}
}
