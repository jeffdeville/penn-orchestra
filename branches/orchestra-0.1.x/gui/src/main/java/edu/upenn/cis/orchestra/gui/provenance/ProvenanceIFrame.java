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
package edu.upenn.cis.orchestra.gui.provenance;

import javax.swing.JInternalFrame;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;

public class ProvenanceIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	ProvenanceViewer _provView;

	public ProvenanceIFrame (Peer p, Schema s, OrchestraSystem sys, RelationDataEditorFactory dataEditFactory)
	{
		super ("Provenance selector", true, true, true, true);
		_provView = new ProvenanceViewer(p, s, sys, dataEditFactory);
		add (_provView);
	}
	
	public void close() {
	}
}
