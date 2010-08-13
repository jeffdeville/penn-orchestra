package edu.upenn.cis.orchestra.datamodel.exceptions;

public class RelationUpdateException extends Exception {

	public static final long serialVersionUID=1L;

	public RelationUpdateException ()
	{
		super ();
	}

	public RelationUpdateException (String msg)
	{
		super (msg);
	}
}
