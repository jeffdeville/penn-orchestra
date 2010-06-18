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
package edu.upenn.cis.orchestra.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class ConfigDialog extends JDialog {
	JPanel _mainPane;

	JTextArea _config;

	private File _file;
	
	public ConfigDialog(File foo, String title) throws IOException {
		_file = foo;
		
		setTitle(title);
		
		init();
	}
	
	public void init() throws IOException {

		setLayout(new BorderLayout());

		/*
		 * Query sub-pane includes label, text area, buttons
		 */
		_mainPane = new JPanel();
		_mainPane.setLayout(new BorderLayout());
		_config = new JTextArea();
		
		_mainPane.setBorder(BorderFactory.createTitledBorder("Configuration file"));
		
		JScrollPane editorScrollPane = new JScrollPane(_config);
        editorScrollPane.setVerticalScrollBarPolicy(
                        JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        editorScrollPane.setHorizontalScrollBarPolicy(
                JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        editorScrollPane.setPreferredSize(new Dimension(400, 600));
        editorScrollPane.setMinimumSize(new Dimension(100, 100));
		
		_mainPane.add(editorScrollPane, BorderLayout.CENTER);
		
		JPanel buttons = new JPanel();
		buttons.setLayout(new GridLayout(1,2));
		final JButton btnSave = new JButton ("Save");
		btnSave.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				ConfigDialog.this.saveConfigFile();
				ConfigDialog.this.setVisible(false);
			}
		});
		buttons.add(btnSave);
		final JButton btnCancel = new JButton ("Cancel");
		btnCancel.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				ConfigDialog.this.setVisible(false);
			}
		});
		buttons.add (btnCancel);
		_mainPane.add(buttons, BorderLayout.SOUTH);

		/*
		 * Assemble the main pane
		 */
		add(_mainPane, BorderLayout.CENTER);
		
		if (_file != null && _file.exists()) {
			BufferedReader f = new BufferedReader(new FileReader(_file));
			
			String str = f.readLine();
			
			while (str != null) {
				_config.append(str + "\n");
				str = f.readLine();
			}
		
			f.close();
		}
		
		pack();
	}

	/**
	 * Select a filename to save to
	 */
	private void saveConfigFile()
	{
		try {
			String fName = _file.getAbsolutePath();//file.getName();
			
			saveConfig(fName);
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "I/O error loading datalog file", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error loading datalog", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		}
	}

	/**
	 * Saves the query text area to the designated file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	private void saveConfig(String filename) throws IOException {
		BufferedWriter f = new BufferedWriter(new FileWriter(filename));

		f.write(_config.getText());
		f.close();
	}

}
