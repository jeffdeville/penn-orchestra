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
package edu.upenn.cis.orchestra.datamodel.exceptions;

/**
 * Exception raised whenever an incomplete/incorrect bean is used to 
 * initialize a model class 
 * @author Olivier Biton
 *
 */
public class InvalidBeanException extends ModelException{
	public static final long serialVersionUID = 1L; 
	
	private Object _bean;
	
	public Object getBean() {
		return _bean;
	}

	public InvalidBeanException (Object bean, String errMsg)
	{
		super (errMsg + "(" + bean.getClass().getName() + ")");
		_bean = bean;
	}
}
