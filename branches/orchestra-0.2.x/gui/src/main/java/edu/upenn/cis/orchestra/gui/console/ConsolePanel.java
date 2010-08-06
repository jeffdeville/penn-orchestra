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
import java.awt.Cursor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.console.Console;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class ConsolePanel extends JPanel {
	public static final long serialVersionUID = 1L;
	
    JTextArea m_textArea = new JTextArea();
    JComboBox m_combobox = new JComboBox();
    JScrollPane m_scrollPane = new JScrollPane(m_textArea);
    Console m_console;

    
    public ConsolePanel(OrchestraSystem peers) {
    	super (new BorderLayout ());

    	
    	// Add a scrolling text area
        m_textArea.setEditable(false);
        m_textArea.setFocusable(false);
        m_textArea.setRows(20);
        m_textArea.setColumns(50);
        add(m_scrollPane, BorderLayout.CENTER);
        add(m_combobox, BorderLayout.SOUTH);
        setVisible(true);

    	TextAreaOutputStream tout = new TextAreaOutputStream(m_textArea);
    	
    	m_console = new Console(System.in, tout, tout);
    	m_console.setCatalog(peers);
		m_console.prompt(true);

		final JPanel panel = this;
		
		ActionListener l = new ActionListener() {
    		public void actionPerformed(ActionEvent e) {
    			if (e.getActionCommand().equals("comboBoxEdited")) {
					String line = ((String)m_combobox.getSelectedItem()).trim();
					m_textArea.append(line);
					m_textArea.append("\n");
					if (line.length() > 0) {
	    				m_combobox.addItem(line);
						Cursor cur = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR);
						panel.setCursor(cur);
						m_textArea.setCursor(cur);
						m_combobox.setCursor(cur);
						m_combobox.setEnabled(false);
						final String[] args = line.split(" ");
						
						new SwingWorker<Void,Void> () 
						{
							@Override
							protected Void doInBackground() throws Exception {
								m_console.processLine(args);
								return null;
							}
							
							@Override
							protected void done() {
								try {
									get();
								} catch (Exception e) {
									m_textArea.append(e.getMessage());
									m_textArea.append("\n");
									if (Config.getDebug()) {
//										e.printStackTrace(new PrintStream(new TextAreaOutputStream(m_textArea)));
										e.printStackTrace();
									}
								}
			    				m_combobox.setSelectedItem("");
								m_console.prompt(false);
								Cursor cur = Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR);
								panel.setCursor(cur);
								m_textArea.setCursor(cur);
								m_combobox.setCursor(cur);
								m_combobox.setEnabled(true);
								m_combobox.requestFocus();
								m_scrollPane.getHorizontalScrollBar().setValue(0);
								m_scrollPane.getVerticalScrollBar().setValue(Integer.MAX_VALUE);
							}
						}.execute();
					}

    			}
    		}
    	};
    	m_combobox.setEditable(true);
    	m_combobox.addActionListener(l);
    }
    
    @Override
    public void requestFocus() {
    	m_combobox.requestFocus();
    }
    
    
    class TextAreaOutputStream extends OutputStream {
    	protected JTextArea m_textarea;
    	protected ByteArrayOutputStream m_bytes;

    	class Updater implements Runnable {
    		protected JTextArea m_textarea;
    		protected String m_str;
    		public Updater(JTextArea textarea, String str) {
    			m_textarea = textarea;
    			m_str = str;
    		}
		    public void run() {
		    	synchronized (getTreeLock()) { synchronized (m_textArea) {
			    	m_textarea.append(m_str); 					
				}					
				}
		    }
		};

    	public TextAreaOutputStream(JTextArea textarea) {
    		m_textarea = textarea;
    		m_bytes = new ByteArrayOutputStream();
    	}
    	
    	protected void update() {
    		Updater u = new Updater(m_textarea, m_bytes.toString());
    		SwingUtilities.invokeLater(u);   		
    		m_bytes.reset();
    	}
    	
    	public synchronized void write(byte[] b) throws IOException {
    		m_bytes.write(b);
    		update();
    	}

    	public synchronized void write(byte[] b, int off, int len) {
    		m_bytes.write(b, off, len);
    		update();
    	}

    	public synchronized void write(int b) {
    		m_bytes.write(b);
    		update();
    	}
    }
	
    
}
