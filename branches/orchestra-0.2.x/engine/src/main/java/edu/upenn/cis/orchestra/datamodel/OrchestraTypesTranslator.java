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
import java.lang.reflect.Field;


/****************************************************************
 * Utility class to get the type label for each JDBC datatype
 * @author Olivier Biton
 *****************************************************************
 */
public class OrchestraTypesTranslator {

	
	/**
	 * Given a JDBC datatype int value as described in <code>java.sql.Types</code>, 
	 * get the name of this datatype (attribute name in <code>java.sql.Types</code>)
	 * @param type Datatype for which the name is needed
	 * @return Datatype name, null if does not exist
	 */
	public static String typeToString (int type)
	{
		String lib = null;
		try
		{
			Field[] fields = ClassLoader.getSystemClassLoader().loadClass("java.sql.Types").getDeclaredFields(); 
			for (int i = 0 ; i < fields.length && lib == null ; i++)
				if (fields[i].getInt(null)==type)
					lib = fields[i].getName();
		} 
		catch (IllegalAccessException ex)
		{
			//TODO: Log / raise exception
			ex.printStackTrace();
		}
		catch (ClassNotFoundException ex)
		{
			//TODO: Log / raise exception
			ex.printStackTrace();
		}		
		return lib;
	}
	
	
	/**
	 * Given a JDBC datatype name (must be an attribute name in <code>java.sql.Types</code>), 
	 * get its int value as described in <code>java.sql.Types</code>
	 * @param type Datatype name for which the code is needed
	 * @return int value associated with the datatype, -1 if not found
	 */
	public static int typeFromString (String type)
	{
		int res = -1;
		try
		{
			res = ClassLoader.getSystemClassLoader().loadClass("java.sql.Types").getField(type).getInt(null);
		}
		catch (NoSuchFieldException ex)
		{
			//TODO: Log / raise exception
			ex.printStackTrace();
		}
		catch (IllegalAccessException ex)
		{
			//TODO: Log / raise exception
			ex.printStackTrace();
		}
		catch (ClassNotFoundException ex)
		{
			//TODO: Log / raise exception
			ex.printStackTrace();
		}	
		return res;
	}
	
	
}
