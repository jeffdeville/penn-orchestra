package edu.upenn.cis.orchestra.datamodel.exceptions;

public class SchemaNotFoundException extends Exception {

	public static final long serialVersionUID=1L;

	public SchemaNotFoundException ()
	{
		super ();
	}

	public SchemaNotFoundException (String msg)
	{
		super (msg);
	}
}
