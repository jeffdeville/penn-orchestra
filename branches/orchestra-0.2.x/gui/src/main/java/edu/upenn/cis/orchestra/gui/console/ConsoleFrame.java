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
package edu.upenn.cis.orchestra.gui.console;

import java.awt.BorderLayout;
import java.io.IOException;

import javax.swing.JComboBox;
import javax.swing.JInternalFrame;
import javax.swing.JTextArea;

import edu.upenn.cis.orchestra.console.Console;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class ConsoleFrame extends JInternalFrame {
    JTextArea m_textArea = new JTextArea();
    JComboBox m_combobox = new JComboBox();
    Console m_console;

    static final long serialVersionUID = 42;
    
    public ConsoleFrame(OrchestraSystem peers, String cdssName) throws IOException {
    	super (cdssName + " console", true, false, true, true);

        getContentPane().add(new ConsolePanel(peers), BorderLayout.CENTER);
        pack ();
    }
}