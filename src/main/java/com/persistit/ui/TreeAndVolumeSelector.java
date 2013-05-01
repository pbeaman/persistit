/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    private static final String PROTOTYPE_VOLUME_NAME = "/this/that/persistit";

    private static final String PROTOTYPE_TREE_NAME = "aTreeName (/var/lib/this/that/other)";

    private final static int VISIBLE_ROW_COUNT = 12;

    AdminUI _adminUI;
    private JList _volumeList;
    private JList _treeList;
    private JList _selectedTreeList;

    private final DefaultListModel _treeListModel = new DefaultListModel();
    private final DefaultListModel _selectedTreeListModel = new DefaultListModel();

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
    public void setup(final AdminUI ui) {
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
            public void valueChanged(final ListSelectionEvent lse) {
                final int index = _volumeList.getSelectedIndex();
                if (!lse.getValueIsAdjusting() && !_refreshing && index >= 0) {
                    final VolumeInfo[] array = (VolumeInfo[]) _volumeInfoArrayModel.getInfoArray();
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
            public void valueChanged(final ListSelectionEvent lse) {
                if (!lse.getValueIsAdjusting() && !_refreshing) {
                }
            }
        });

        _volumeList.setVisibleRowCount(VISIBLE_ROW_COUNT);
        _treeList.setVisibleRowCount(VISIBLE_ROW_COUNT);
        _selectedTreeList.setVisibleRowCount(VISIBLE_ROW_COUNT);

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.anchor = GridBagConstraints.WEST;

        final JPanel buttonPanel = new JPanel(new GridBagLayout());

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

    private boolean equals(final Object a, final Object b) {
        if (a == null)
            return b == null;
        else
            return a.equals(b);
    }

    private void selectVolume(final VolumeInfo volumeInfo) {
        final String newName = volumeInfo == null ? null : volumeInfo.getName();
        if (!equals(newName, _selectedVolumeName)) {
            _selectedVolumeName = newName;
        }
    }

    @Override
    protected void refresh(final boolean reset) {
        synchronized (this) {
            if (_refreshing)
                return;
            _refreshing = true;
        }
        try {
            final Management management = _adminUI.getManagement();
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
        } catch (final RemoteException re) {
            _adminUI.postException(re);
        } finally {
            synchronized (this) {
                _refreshing = false;
            }
        }
    }

    private void updateTreeListModel(final String volumeName, final TreeInfo[] array) {
        _treeListModel.clear();
        if (array == null)
            return;
        for (int index = 0; index < array.length; index++) {
            final TreeInfo info = array[index];
            final TreeItem item = new TreeItem();
            item._volumeName = volumeName;
            item._treeName = info.getName();
            final int selectedSize = _selectedTreeListModel.size();
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
        public boolean equals(final Object obj) {
            if (!(obj instanceof TreeItem))
                return false;
            final TreeItem ti = (TreeItem) obj;
            return ti._treeName.equals(_treeName) && ti._volumeName.equals(_volumeName);
        }
    }

    private static class TreeItemListCellRenderer extends DefaultListCellRenderer {

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

        TreeItemListCellRenderer(final boolean supplySide, final JList list) {
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
        public Component getListCellRendererComponent(final JList list, final Object value, final int index,
                final boolean isSelected, final boolean cellHasFocus) {
            if (!(value instanceof TreeItem)) {
                setText(value.toString());

                setBackground(isSelected ? _enabledSelectedBackground : _defaultBackground);

                setForeground(isSelected ? _enabledSelectedForeground : _defaultForeground);
            } else {
                final TreeItem item = (TreeItem) value;
                sb.setLength(0);
                sb.append(item._treeName);
                sb.append(" (");
                sb.append(item._volumeName);
                sb.append(")");
                setText(sb.toString());

                final boolean enabled = item._selected ^ _supplySide;

                final Color background = isSelected ? (enabled ? _enabledSelectedBackground
                        : _disabledSelectedBackground) : (enabled ? _defaultBackground : _disabledBackground);

                final Color foreground = isSelected ? (enabled ? _enabledSelectedForeground
                        : _disabledSelectedForeground) : (enabled ? _defaultForeground : _disabledForeground);

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
    public void actionPerformed(final AdminAction action, final ActionEvent ae) {
        final String name = action.getName();
        if ("ADD_SELECTED".equals(name)) {
            final int[] selected = _treeList.getSelectedIndices();
            for (int index = 0; index < selected.length; index++) {
                final int itemIndex = selected[index];
                final TreeItem item = (TreeItem) _treeListModel.getElementAt(itemIndex);
                if (!item._selected) {
                    item._selected = true;
                    _selectedTreeListModel.addElement(item);
                }
            }
            _treeList.repaint();
        } else if ("REMOVE_SELECTED".equals(name)) {
            final int[] selected = _selectedTreeList.getSelectedIndices();
            for (int index = selected.length; --index >= 0;) {
                final int itemIndex = selected[index];
                final TreeItem item = (TreeItem) _selectedTreeListModel.getElementAt(itemIndex);
                if (item._selected) {
                    item._selected = false;
                    _selectedTreeListModel.removeElementAt(itemIndex);
                }
            }
            _treeList.repaint();
        } else if ("ADD_ALL".equals(name)) {
            final int size = _treeListModel.size();
            for (int index = 0; index < size; index++) {
                final TreeItem item = (TreeItem) _treeListModel.getElementAt(index);
                if (!item._selected) {
                    item._selected = true;
                    _selectedTreeListModel.addElement(item);
                }
            }
            _treeList.repaint();
        } else if ("REMOVE_ALL".equals(name)) {
            final int size = _selectedTreeListModel.size();
            for (int index = size; --index >= 0;) {
                final TreeItem item = (TreeItem) _selectedTreeListModel.getElementAt(index);
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
            final int volumeCount = _volumeList.getModel().getSize();
            final Management management = _adminUI.getManagement();
            if (management != null) {
                for (int volumeIndex = 0; volumeIndex < volumeCount; volumeIndex++) {
                    final VolumeInfo volumeInfo = (VolumeInfo) _volumeList.getModel().getElementAt(volumeIndex);
                    final String volumeName = volumeInfo.getName();
                    try {
                        final TreeInfo[] treeInfoArray = management.getTreeInfoArray(volumeName);

                        for (int treeIndex = 0; treeIndex < treeInfoArray.length; treeIndex++) {
                            final TreeItem item = new TreeItem();
                            item._volumeName = volumeName;
                            item._treeName = treeInfoArray[treeIndex].getName();
                            item._selected = true;
                            _selectedTreeListModel.addElement(item);
                        }
                    } catch (final RemoteException re) {
                        _adminUI.postException(re);
                        break;
                    }
                }
            }
        }
    }

    public String getTreeListString() {
        final StringBuilder sb = new StringBuilder();
        final int size = _selectedTreeListModel.size();
        for (int index = 0; index < size; index++) {
            final TreeItem item = (TreeItem) _selectedTreeListModel.elementAt(index);
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(quote(item._volumeName));
            sb.append(':');
            sb.append(quote(item._treeName));
        }
        return sb.toString();
    }

    private String quote(final String s) {
        if (s.indexOf('\\') == 0 && s.indexOf(',') == 0 && s.indexOf(';') == 0) {
            return s;
        }
        final StringBuilder sb = new StringBuilder();
        for (int index = 0; index < s.length(); index++) {
            final char c = s.charAt(index);
            if (c == '\\' || c == ',' || c == ';') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
