/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;

import javax.swing.Box;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListCellRenderer;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.persistit.Management;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;
import com.persistit.ui.AdminUI.AdminAction;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class TreeAndVolumeSelector extends AdminPanel implements AdminCommand {

    private static final long serialVersionUID = 1;

    private static final String PROTOTYPE_VOLUME_NAME = "c:/this/that/persistit";

    private static final String PROTOTYPE_TREE_NAME = "aTreeName (c:/this/that/other)";

    private final static int VISIBLE_ROW_COUNT = 12;

    AdminUI _adminUI;
    private JList _volumeList;
    private JList _treeList;
    private JList _selectedTreeList;

    private DefaultListModel _treeListModel = new DefaultListModel();
    private DefaultListModel _selectedTreeListModel = new DefaultListModel();

    private JButton _addAllAllButton;
    private JButton _addSelectedButton;
    private JButton _removeSelectedButton;
    private JButton _addAllButton;
    private JButton _removeAllButton;

    private AdminAction _addAllAllAction;
    private AdminAction _addSelectedAction;
    private AdminAction _removeSelectedAction;
    private AdminAction _addAllAction;
    private AdminAction _removeAllAction;

    private ManagementListModel _volumeInfoArrayModel;
    private String _selectedVolumeName;

    private boolean _refreshing;

    @Override
    public void setup(AdminUI ui) {
        _adminUI = ui;

        _addSelectedAction = ui.createAction(this, _adminUI.getProperty("TVSelector.AddSelected"));

        _removeSelectedAction = ui.createAction(this, _adminUI.getProperty("TVSelector.RemoveSelected"));

        _addAllAction = ui.createAction(this, _adminUI.getProperty("TVSelector.AddAll"));

        _addAllAllAction = ui.createAction(this, _adminUI.getProperty("TVSelector.AddAllAll"));

        _removeAllAction = ui.createAction(this, _adminUI.getProperty("TVSelector.RemoveAll"));

        _addAllAllButton = new JButton(_addAllAllAction);
        _addSelectedButton = new JButton(_addSelectedAction);
        _removeSelectedButton = new JButton(_removeSelectedAction);
        _addAllButton = new JButton(_addAllAction);
        _removeAllButton = new JButton(_removeAllAction);

        _volumeInfoArrayModel = new ManagementListModel();
        _volumeList = new JList(_volumeInfoArrayModel);
        _volumeList.setPrototypeCellValue(PROTOTYPE_VOLUME_NAME);

        _treeList = new JList(_treeListModel);
        _treeList.setPrototypeCellValue(PROTOTYPE_TREE_NAME);
        _treeList.setCellRenderer(new TreeItemListCellRenderer(true, _treeList));

        _selectedTreeList = new JList(_selectedTreeListModel);
        _selectedTreeList.setPrototypeCellValue(PROTOTYPE_TREE_NAME);
        _selectedTreeList.setCellRenderer(new TreeItemListCellRenderer(false, _selectedTreeList));

        _volumeList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        _volumeList.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent lse) {
                int index = _volumeList.getSelectedIndex();
                if (!lse.getValueIsAdjusting() && !_refreshing && index >= 0) {
                    VolumeInfo[] array = (VolumeInfo[]) _volumeInfoArrayModel.getInfoArray();
                    if (array != null && index < array.length) {
                        selectVolume(array[index]);
                    } else {
                        selectVolume(null);
                    }
                    refresh(false);
                }
            }
        });

        _treeList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        _treeList.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent lse) {
                if (!lse.getValueIsAdjusting() && !_refreshing) {
                }
            }
        });

        _volumeList.setVisibleRowCount(VISIBLE_ROW_COUNT);
        _treeList.setVisibleRowCount(VISIBLE_ROW_COUNT);
        _selectedTreeList.setVisibleRowCount(VISIBLE_ROW_COUNT);

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.anchor = GridBagConstraints.WEST;

        JPanel buttonPanel = new JPanel(new GridBagLayout());

        buttonPanel.add(_addAllAllButton, gbc);
        gbc.gridy++;

        buttonPanel.add(Box.createVerticalStrut(8), gbc);
        gbc.gridy++;

        buttonPanel.add(_addAllButton, gbc);
        gbc.gridy++;

        buttonPanel.add(_addSelectedButton, gbc);
        gbc.gridy++;

        buttonPanel.add(Box.createVerticalStrut(8), gbc);
        gbc.gridy++;

        buttonPanel.add(_removeSelectedButton, gbc);
        gbc.gridy++;

        buttonPanel.add(_removeAllButton, gbc);
        gbc.gridy++;

        buttonPanel.add(Box.createVerticalStrut(8), gbc);

        gbc.gridx = 0;
        gbc.gridy = 0;

        setLayout(new GridBagLayout());
        add(new JScrollPane(_volumeList), gbc);
        gbc.gridx++;
        add(new JScrollPane(_treeList), gbc);
        gbc.gridx++;
        add(buttonPanel, gbc);
        gbc.weightx = 1.0;
        gbc.gridx++;
        add(new JScrollPane(_selectedTreeList), gbc);
    }

    private boolean equals(Object a, Object b) {
        if (a == null)
            return b == null;
        else
            return a.equals(b);
    }

    private void selectVolume(VolumeInfo volumeInfo) {
        String newName = volumeInfo == null ? null : volumeInfo.getName();
        if (!equals(newName, _selectedVolumeName)) {
            _selectedVolumeName = newName;
        }
    }

    @Override
    protected void refresh(boolean reset) {
        synchronized (this) {
            if (_refreshing)
                return;
            _refreshing = true;
        }
        try {
            Management management = _adminUI.getManagement();
            if (reset) {
                _selectedVolumeName = null;
            }

            VolumeInfo[] volumeInfoArray = null;
            if (management != null) {
                volumeInfoArray = management.getVolumeInfoArray();
            }
            _volumeInfoArrayModel.setInfoArray(volumeInfoArray);

            TreeInfo[] treeInfoArray = null;
            if (management != null && _selectedVolumeName != null) {
                treeInfoArray = management.getTreeInfoArray(_selectedVolumeName);
            }
            updateTreeListModel(_selectedVolumeName, treeInfoArray);
        } catch (RemoteException re) {
            _adminUI.postException(re);
        } finally {
            synchronized (this) {
                _refreshing = false;
            }
        }
    }

    private void updateTreeListModel(String volumeName, TreeInfo[] array) {
        _treeListModel.clear();
        if (array == null)
            return;
        for (int index = 0; index < array.length; index++) {
            TreeInfo info = array[index];
            TreeItem item = new TreeItem();
            item._volumeName = volumeName;
            item._treeName = info.getName();
            int selectedSize = _selectedTreeListModel.size();
            boolean alreadySelected = false;
            for (int selectedIndex = 0; selectedIndex < selectedSize; selectedIndex++) {
                if (_selectedTreeListModel.getElementAt(selectedIndex).equals(item)) {
                    alreadySelected = true;
                    break;
                }
            }
            item._selected = alreadySelected;
            _treeListModel.addElement(item);
        }
    }

    private static class TreeItem {
        String _treeName;
        String _volumeName;
        boolean _selected;

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TreeItem))
                return false;
            TreeItem ti = (TreeItem) obj;
            return ti._treeName.equals(_treeName) && ti._volumeName.equals(_volumeName);
        }
    }

    private static class TreeItemListCellRenderer extends DefaultListCellRenderer implements ListCellRenderer {

        private static final long serialVersionUID = 1L;
        
        boolean _supplySide;
        StringBuilder sb = new StringBuilder();

        Color _defaultBackground;
        Color _defaultForeground;
        Color _disabledForeground;
        Color _disabledBackground;
        Color _enabledSelectedBackground;
        Color _enabledSelectedForeground;
        Color _disabledSelectedBackground;
        Color _disabledSelectedForeground;

        TreeItemListCellRenderer(boolean supplySide, JList list) {
            super();
            _supplySide = supplySide;

            _defaultBackground = list.getBackground();
            _defaultForeground = list.getForeground();

            _disabledForeground = Color.gray;
            _disabledBackground = _defaultBackground;

            _enabledSelectedBackground = list.getSelectionBackground();
            _enabledSelectedForeground = list.getSelectionForeground();

            _disabledSelectedBackground = Color.lightGray;
            _disabledSelectedForeground = Color.darkGray;

        }

        @Override
        public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected,
                boolean cellHasFocus) {
            if (!(value instanceof TreeItem)) {
                setText(value.toString());

                setBackground(isSelected ? _enabledSelectedBackground : _defaultBackground);

                setForeground(isSelected ? _enabledSelectedForeground : _defaultForeground);
            } else {
                TreeItem item = (TreeItem) value;
                sb.setLength(0);
                sb.append(item._treeName);
                sb.append(" (");
                sb.append(item._volumeName);
                sb.append(")");
                setText(sb.toString());

                boolean enabled = item._selected ^ _supplySide;

                Color background = isSelected ? (enabled ? _enabledSelectedBackground : _disabledSelectedBackground)
                        : (enabled ? _defaultBackground : _disabledBackground);

                Color foreground = isSelected ? (enabled ? _enabledSelectedForeground : _disabledSelectedForeground)
                        : (enabled ? _defaultForeground : _disabledForeground);

                setBackground(background);
                setForeground(foreground);
            }
            return this;
        }
    }

    @Override
    public Map getMenuMap() {
        return Collections.EMPTY_MAP;
    }

    @Override
    public void setDefaultButton() {
    }

    @Override
    public void actionPerformed(AdminAction action, ActionEvent ae) {
        String name = action.getName();
        if ("ADD_SELECTED".equals(name)) {
            int[] selected = _treeList.getSelectedIndices();
            for (int index = 0; index < selected.length; index++) {
                int itemIndex = selected[index];
                TreeItem item = (TreeItem) _treeListModel.getElementAt(itemIndex);
                if (!item._selected) {
                    item._selected = true;
                    _selectedTreeListModel.addElement(item);
                }
            }
            _treeList.repaint();
        } else if ("REMOVE_SELECTED".equals(name)) {
            int[] selected = _selectedTreeList.getSelectedIndices();
            for (int index = selected.length; --index >= 0;) {
                int itemIndex = selected[index];
                TreeItem item = (TreeItem) _selectedTreeListModel.getElementAt(itemIndex);
                if (item._selected) {
                    item._selected = false;
                    _selectedTreeListModel.removeElementAt(itemIndex);
                }
            }
            _treeList.repaint();
        } else if ("ADD_ALL".equals(name)) {
            int size = _treeListModel.size();
            for (int index = 0; index < size; index++) {
                TreeItem item = (TreeItem) _treeListModel.getElementAt(index);
                if (!item._selected) {
                    item._selected = true;
                    _selectedTreeListModel.addElement(item);
                }
            }
            _treeList.repaint();
        } else if ("REMOVE_ALL".equals(name)) {
            int size = _selectedTreeListModel.size();
            for (int index = size; --index >= 0;) {
                TreeItem item = (TreeItem) _selectedTreeListModel.getElementAt(index);
                if (item._selected) {
                    item._selected = false;
                    _selectedTreeListModel.removeElementAt(index);
                }
            }
            _treeList.repaint();
        } else if ("ADD_ALL_ALL".equals(name)) {
            _volumeList.clearSelection();
            _treeListModel.clear();
            _selectedTreeListModel.clear();
            int volumeCount = _volumeList.getModel().getSize();
            Management management = _adminUI.getManagement();
            if (management != null) {
                for (int volumeIndex = 0; volumeIndex < volumeCount; volumeIndex++) {
                    VolumeInfo volumeInfo = (VolumeInfo) _volumeList.getModel().getElementAt(volumeIndex);
                    String volumeName = volumeInfo.getName();
                    try {
                        TreeInfo[] treeInfoArray = management.getTreeInfoArray(volumeName);

                        for (int treeIndex = 0; treeIndex < treeInfoArray.length; treeIndex++) {
                            TreeItem item = new TreeItem();
                            item._volumeName = volumeName;
                            item._treeName = treeInfoArray[treeIndex].getName();
                            item._selected = true;
                            _selectedTreeListModel.addElement(item);
                        }
                    } catch (RemoteException re) {
                        _adminUI.postException(re);
                        break;
                    }
                }
            }
        }
    }

    public String getTreeListString() {
        StringBuilder sb = new StringBuilder();
        int size = _selectedTreeListModel.size();
        for (int index = 0; index < size; index++) {
            TreeItem item = (TreeItem) _selectedTreeListModel.elementAt(index);
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(quote(item._volumeName));
            sb.append(':');
            sb.append(quote(item._treeName));
        }
        return sb.toString();
    }

    private String quote(String s) {
        if (s.indexOf('\\') == 0 && s.indexOf(',') == 0 && s.indexOf(';') == 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < s.length(); index++) {
            char c = s.charAt(index);
            if (c == '\\' || c == ',' || c == ';') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
