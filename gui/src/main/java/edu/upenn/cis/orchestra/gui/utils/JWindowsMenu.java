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
package edu.upenn.cis.orchestra.gui.utils;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ContainerEvent;
import java.awt.event.ContainerListener;
import java.awt.event.KeyEvent;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.DefaultDesktopManager;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JMenu;
import javax.swing.JMenuItem;

/**
 * Based on the WindowManager class from the Zeus JCL library/guydavis
 * 
 * 
 */
public class JWindowsMenu extends JMenu implements ContainerListener 
{
	public static final long serialVersionUID = 1L;
	
	
    /**
     * The possible static menu items that can be added to the Windows menu
     * above the dynamic listing of open windows.
     */
    enum MenuItem {
        /** Cascade windows from top left down */
        CASCADE,
        /** Checkerboard style tile of windows */
        TILE,
        /** Tile from top down */
        TILE_HORIZ,
        /** Tile from left to right */
        TILE_VERT,
        /** Restore the currently selected window to original size */
        RESTORE,
        /** Restore all windows to their original size */
        RESTORE_ALL,
        /** Minimize the currently selected window to original size */
        MINIMIZE,
        /** Minimize all windows to their original size */
        MINIMIZE_ALL,
        /** Maximize the currently selected window to original size */
        MAXIMIZE,
        /** Maximize all windows to their original size */
        MAXIMIZE_ALL,
        /** Indicates a menu separator should be placed in the menu */
        SEPARATOR
    };

    /** The desktop whose windows are being monitored */
    private JDesktopPane _desktop;

    /** Used to retrieve the menu item corresponding to a given frame */
    private Map<JInternalFrame, JCheckBoxMenuItem> _menusForFrames;

    /** Used for sorting the frames in alphabetical order by title */
    private Comparator<JInternalFrame> _frameComparator;

    /** The static menus for each chosen MenuItem type */
    private Map<MenuItem, JMenuItem> _staticMenus;

    /**
     * Create the "Windows" menu for a MDI view using default title and menu
     * choices.
     * 
     * @param desktop
     *            The desktop to monitor.
     */
    public JWindowsMenu(JDesktopPane desktop) {
        this("Windows", desktop);
    }

    /**
     * Create the "Windows" menu for a MDI view using the given title and
     * default menu choices.
     * 
     * @param windowTitle
     *            The title of the window to display.
     * @param desktop
     *            The desktop to monitor.
     */
    public JWindowsMenu(String windowTitle, JDesktopPane desktop) {
        this(windowTitle, desktop, MenuItem.CASCADE, MenuItem.TILE,
                MenuItem.TILE_HORIZ, MenuItem.TILE_VERT, MenuItem.SEPARATOR,
                MenuItem.RESTORE, MenuItem.MINIMIZE, MenuItem.MAXIMIZE,
                MenuItem.SEPARATOR, MenuItem.RESTORE_ALL,
                MenuItem.MINIMIZE_ALL, MenuItem.MAXIMIZE_ALL);
    }

    /**
     * Create the "Windows" menu for a MDI view using the given title and menu
     * items.
     * 
     * @param windowTitle
     *            The title of the window to display.
     * @param desktop
     *            The desktop to monitor.
     * @param items
     *            A variable length argument indicating which menu items to
     *            display in the menu.
     */
    public JWindowsMenu(String windowTitle, JDesktopPane desktop,
            MenuItem... items) {

        this._desktop = desktop;
        this._staticMenus = new HashMap<MenuItem, JMenuItem>();
        setText(windowTitle);

        for (MenuItem item : items) {
            addMenuItem(item);
        }

        // Add a final separator if the user forgot to include it
        if (items[items.length - 1] != MenuItem.SEPARATOR) {
            addMenuItem(MenuItem.SEPARATOR);
        }

        // Sort frames by title alphabetically
        this._frameComparator = new Comparator<JInternalFrame>() {
            public int compare(JInternalFrame o1, JInternalFrame o2) {
                int ret = 0;
                if (o1 != null && o2 != null) {
                    String t1 = o1.getTitle();
                    String t2 = o2.getTitle();

                    if (t1 != null && t2 != null) {
                        ret = t1.compareTo(t2);
                    } else if (t1 == null && t2 != null) {
                        ret = -1;
                    } else if (t1 != null && t2 == null) {
                        ret = 1;
                    } else {
                        ret = 0;
                    }
                }
                return (ret);
            }
        };

        this._menusForFrames = new HashMap<JInternalFrame, JCheckBoxMenuItem>();
        this._desktop.addContainerListener(this);
        this._desktop.setDesktopManager(new CustomDesktopManager());
        updateWindowsList(); // Setup list for any existing windows
    }

    /**
     * Creates a static menu item with mnemonic and action listener.
     * 
     * @param item
     *            The type of menu item to add.
     */
    private void addMenuItem(MenuItem item) {
        String name = null;
        int mnemonic = 0;
        ActionListener listener = null;

        switch (item) {
        case CASCADE:
            name = "Cascade";
            mnemonic = KeyEvent.VK_C;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    cascade();
                }
            };
            break;
        case TILE:
            name = "Tile";
            mnemonic = KeyEvent.VK_T;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    tile();
                }
            };
            break;
        case TILE_HORIZ:
            name = "Tile Horizontally";
            mnemonic = KeyEvent.VK_H;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    tileHorizontally();
                }
            };
            break;
        case TILE_VERT:
            name = "Tile Vertically";
            mnemonic = KeyEvent.VK_V;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    tileVertically();
                }
            };
            break;
        case RESTORE:
            name = "Restore";
            mnemonic = KeyEvent.VK_R;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    try {
                        if (_desktop.getSelectedFrame().isIcon()) {
                            _desktop.getSelectedFrame().setIcon(false);
                        } else if (_desktop.getSelectedFrame().isMaximum()) {
                            _desktop.getSelectedFrame().setMaximum(false);
                        }
                    } catch (PropertyVetoException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            break;
        case RESTORE_ALL:
            name = "Restore All";
            mnemonic = KeyEvent.VK_E;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    for (JInternalFrame frame : _desktop.getAllFrames()) {
                        try {
                            if (frame.isIcon()) {
                                frame.setIcon(false);
                            } else if (frame.isMaximum()) {
                                frame.setMaximum(false);
                            }
                        } catch (PropertyVetoException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            };
            break;
        case MINIMIZE:
            name = "Minimize";
            mnemonic = KeyEvent.VK_M;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    if (!_desktop.getSelectedFrame().isIcon()) {
                        try {
                            _desktop.getSelectedFrame().setIcon(true);
                        } catch (PropertyVetoException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            };
            break;
        case MINIMIZE_ALL:
            name = "Minimize All";
            mnemonic = KeyEvent.VK_I;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    for (JInternalFrame frame : _desktop.getAllFrames()) {
                        if (!frame.isIcon()) {
                            try {
                                frame.setIcon(true);
                            } catch (PropertyVetoException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                }
            };
            break;
        case MAXIMIZE:
            name = "Maximize";
            mnemonic = KeyEvent.VK_A;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    try {
                        _desktop.getSelectedFrame().setMaximum(true);
                    } catch (PropertyVetoException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            break;
        case MAXIMIZE_ALL:
            name = "Maximize All";
            mnemonic = KeyEvent.VK_X;
            listener = new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    for (JInternalFrame frame : _desktop.getAllFrames()) {
                        try {
                            frame.setMaximum(true);
                        } catch (PropertyVetoException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            };
            break;
        case SEPARATOR:
            addSeparator(); // Create a menu separator
        }

        // Now create a menu item with the given name, mnemonic and listener
        if (name != null) {
            JMenuItem menuItem = new JMenuItem(name);
            menuItem.setMnemonic(mnemonic);
            menuItem.addActionListener(listener);
            add(menuItem); // Add to the main menu
            _staticMenus.put(item, menuItem);
        }
    }

    /**
     * @return A list of frames on the desktop which are not iconified and are
     *         visible.
     */
    private List<JInternalFrame> getAllVisibleFrames() {
        List<JInternalFrame> frames = new ArrayList<JInternalFrame>();
        for (JInternalFrame frame : this._desktop.getAllFrames()) {
            if (frame.isVisible() && !frame.isClosed() && !frame.isIcon()) {
                frames.add(frame);
            }
        }
        Collections.sort(frames, this._frameComparator);
        return frames;
    }

    /**
     * Change the bounds of visible windows to tile them vertically on the
     * desktop.
     */
    protected void tileVertically() {
        List<JInternalFrame> frames = getAllVisibleFrames();
        int newWidth = this._desktop.getWidth() / frames.size();
        int newHeight = this._desktop.getHeight();

        int x = 0;
        for (JInternalFrame frame : frames) {
            if (frame.isMaximum()) {
                try {
                    frame.setMaximum(false); // Restore if maximized first
                } catch (PropertyVetoException ex) {
                    throw new RuntimeException(ex);
                }
            }
            frame.reshape(x, 0, newWidth, newHeight);
            x += newWidth;
        }
    }

    /**
     * Change the bounds of visible windows to tile them horizontally on the
     * desktop.
     */
    protected void tileHorizontally() {
        List<JInternalFrame> frames = getAllVisibleFrames();
        int newWidth = this._desktop.getWidth();
        int newHeight = this._desktop.getHeight() / frames.size();

        int y = 0;
        for (JInternalFrame frame : frames) {
            if (frame.isMaximum()) {
                try {
                    frame.setMaximum(false); // Restore if maximized first
                } catch (PropertyVetoException ex) {
                    throw new RuntimeException(ex);
                }
            }
            frame.reshape(0, y, newWidth, newHeight);
            y += newHeight;
        }
    }

    /**
     * Change the bounds of visible windows to tile them checkerboard-style on
     * the desktop.
     */
    protected void tile() {
        List<JInternalFrame> frames = getAllVisibleFrames();
        if (frames.size() == 0) {
            return;
        }

        double sqrt = Math.sqrt(frames.size());
        int numCols = (int) Math.floor(sqrt);
        int numRows = numCols;
        if ((numCols * numRows) < frames.size()) {
            numCols++;
            if ((numCols * numRows) < frames.size()) {
                numRows++;
            }
        }

        int newWidth = this._desktop.getWidth() / numCols;
        int newHeight = this._desktop.getHeight() / numRows;

        int y = 0;
        int x = 0;
        int frameIdx = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                if (frameIdx < frames.size()) {
                    JInternalFrame frame = frames.get(frameIdx++);
                    if (frame.isMaximum()) {
                        try {
                            frame.setMaximum(false);
                        } catch (PropertyVetoException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    frame.reshape(x, y, newWidth, newHeight);
                    x += newWidth;
                }
            }
            x = 0;
            y += newHeight;
        }
    }

    /**
     * Change the bounds of visible windows to cascade them down from the top
     * left of the desktop.
     */
    protected void cascade() {
        List<JInternalFrame> frames = getAllVisibleFrames();
        if (frames.size() == 0) {
            return;
        }

        int newWidth = (int) (this._desktop.getWidth() * 0.6);
        int newHeight = (int) (this._desktop.getHeight() * 0.6);
        int x = 0;
        int y = 0;
        for (JInternalFrame frame : frames) {
            if (frame.isMaximum()) {
                try {
                    frame.setMaximum(false);
                } catch (PropertyVetoException ex) {
                    throw new RuntimeException(ex);
                }
            }
            frame.reshape(x, y, newWidth, newHeight);
            x += 25;
            y += 25;

            if ((x + newWidth) > this._desktop.getWidth()) {
                x = 0;
            }

            if ((y + newHeight) > this._desktop.getHeight()) {
                y = 0;
            }
        }
    }

    /**
     * Records the addition of a window to the desktop.
     * 
     * @see java.awt.event.ContainerListener#componentAdded(java.awt.event.ContainerEvent)
     */
    public void componentAdded(ContainerEvent e) {
        updateWindowsList();
    }

    /**
     * Records the removal of a window from the desktop.
     * 
     * @see java.awt.event.ContainerListener#componentRemoved(java.awt.event.ContainerEvent)
     */
    public void componentRemoved(@SuppressWarnings("unused")
    ContainerEvent e) {
        updateWindowsList();
    }

    /**
     * Invoked to regenerate the dynamic window listing menu items at the bottom
     * of the menu.
     */
    private void updateWindowsList() {

        List<JInternalFrame> frames = new ArrayList<JInternalFrame>();
        for (JInternalFrame frame : this._desktop.getAllFrames()) {
            frames.add(frame);
        }
        Collections.sort(frames, this._frameComparator);

        for (Component menu : this.getMenuComponents()) {
            if (menu instanceof JCheckBoxMenuItem) {
                this.remove(menu);
            }
        }

        this._menusForFrames.clear();

        int i = 1;
        ButtonGroup group = new ButtonGroup();
        for (final JInternalFrame frame : frames) {
            JCheckBoxMenuItem item = new JCheckBoxMenuItem(i + " "
                    + frame.getTitle());

            if (frame.isIcon()) {
                item.setSelected(false);
            }

            if (frame.isSelected()) {
                item.setState(true);
            }
            group.add(item);
            item.addActionListener(new ActionListener() {
                public void actionPerformed(@SuppressWarnings("unused")
                ActionEvent e) {
                    try {
                        if (frame.isIcon()) {
                            frame.setIcon(false);
                        }

                        if (!frame.isSelected()) {
                            frame.setSelected(true);
                            frame.toFront();
                        }
                    } catch (PropertyVetoException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
            this._menusForFrames.put(frame, item);
            add(item);
            i++;
        }
    }

    /**
     * Toggle the enabled state of the static menu items depending on the
     * selected frame.
     */
    private void updateStaticMenuItems() {
        JInternalFrame selectedFrame = this._desktop.getSelectedFrame();
        JMenuItem minimizeItem = this._staticMenus.get(MenuItem.MINIMIZE);
        JMenuItem maximizeItem = this._staticMenus.get(MenuItem.MAXIMIZE);
        JMenuItem restoreItem = this._staticMenus.get(MenuItem.RESTORE);

        for (JCheckBoxMenuItem item : _menusForFrames.values()) {
            item.setSelected(false);
        }

        if (selectedFrame == null) {
            restoreItem.setEnabled(false);
            maximizeItem.setEnabled(false);
            minimizeItem.setEnabled(false);
        } else if (selectedFrame.isIcon()) {
            restoreItem.setEnabled(true);
            maximizeItem.setEnabled(selectedFrame.isMaximizable());
            minimizeItem.setEnabled(false);
            _menusForFrames.get(selectedFrame).setSelected(true);
        } else if (selectedFrame.isMaximum()) {
            restoreItem.setEnabled(true);
            maximizeItem.setEnabled(false);
            minimizeItem.setEnabled(selectedFrame.isIconifiable());
            _menusForFrames.get(selectedFrame).setSelected(true);
        } else { // Window in regular position
            restoreItem.setEnabled(false);
            maximizeItem.setEnabled(selectedFrame.isMaximizable());
            minimizeItem.setEnabled(selectedFrame.isIconifiable());
            _menusForFrames.get(selectedFrame).setSelected(true);
        }
    }

    /**
     * A desktop manager for listening to window-related events on the desktop.
     */
    private class CustomDesktopManager extends DefaultDesktopManager 
    {
    	public static final long serialVersionUID = 1L;
    	
        /**
         * @see javax.swing.DefaultDesktopManager#activateFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void activateFrame(JInternalFrame f) {
            super.activateFrame(f);
            updateStaticMenuItems();
        }

        /**
         * @see javax.swing.DefaultDesktopManager#deactivateFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void deactivateFrame(JInternalFrame f) {
            super.deactivateFrame(f);
            updateStaticMenuItems();
        }

        /**
         * @see javax.swing.DefaultDesktopManager#deiconifyFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void deiconifyFrame(JInternalFrame f) {
            super.deiconifyFrame(f);
            updateStaticMenuItems();
        }

        /**
         * @see javax.swing.DefaultDesktopManager#iconifyFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void iconifyFrame(JInternalFrame f) {
            super.iconifyFrame(f);
            updateStaticMenuItems();
        }

        /**
         * @see javax.swing.DefaultDesktopManager#maximizeFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void maximizeFrame(JInternalFrame f) {
            super.maximizeFrame(f);
            updateStaticMenuItems();
        }

        /**
         * @see javax.swing.DefaultDesktopManager#minimizeFrame(javax.swing.JInternalFrame)
         */
        @Override
        public void minimizeFrame(JInternalFrame f) {
            super.minimizeFrame(f);
            updateStaticMenuItems();
        }
    }

}