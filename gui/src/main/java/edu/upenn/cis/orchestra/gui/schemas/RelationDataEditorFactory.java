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
package edu.upenn.cis.orchestra.gui.schemas;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class RelationDataEditorFactory {

	private final RelationDataEditorIntf _dataEdit; 
	
	public RelationDataEditorFactory (OrchestraSystem sys)
	{
		if (sys.getRecMode())
			_dataEdit = getReconInstance(sys);
		else
			_dataEdit = getUpdTransInstance(sys);
	}

	public RelationDataEditorIntf getInstance (OrchestraSystem sys)
	{
		return _dataEdit;
	}

	private static RelationDataEditorIntf getReconInstance (OrchestraSystem sys)
	{
		return new RelationDataEditor (sys);
	}

	private static RelationDataEditorIntf getUpdTransInstance (OrchestraSystem sys)
	{
		return new RelationDataEditorUpdTrans (sys);
	}

}
