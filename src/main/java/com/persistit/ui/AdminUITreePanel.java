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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.persistit.Management;

public class AdminUITreePanel extends AdminPanel implements AdminCommand {
    AdminUI _adminUI;
    private static final String PROTOTYPE_VOLUME_NAME = "c:/this/that/and/the/other/persistit";

    private static double TOP_SPLITPANE_RESIZE_WEIGHT = .25;
    private static double BOTTOM_SPLITPANE_RESIZE_WEIGHT = .85;
    private static double MAIN_SPLITPANE_RESIZE_WEIGHT = .25;

    private ManagementListModel _volumeInfoArrayModel;
    private ManagementTableModel _treeInfoArrayModel;
    private ManagementSlidingTableModel _logicalRecordArrayModel;

    private final Map _menuMap = new TreeMap();

    private boolean _refreshing;

    private JPanel _volumePanel;
    private JPanel _treePanel;
    private JPanel _dataPanel;
    private InspectorPanel _inspectorPanel;
    private AdminUI.AdminAction _displayAction;
    private JTextField _filterTextField;

    private JList _volumeList;
    private JTable _treeTable;
    private JTable _dataTable;

    private String _selectedVolumeName;
    private String _selectedTreeName;

    private JButton _displayButton;

    @Override
    protected void setup(final AdminUI ui) throws NoSuchMethodException, RemoteException {
        _adminUI = ui;
        _volumePanel = new JPanel(new BorderLayout());
        _treePanel = new JPanel(new BorderLayout());
        _dataPanel = new JPanel(new BorderLayout());
        _inspectorPanel = new InspectorPanel(ui);

        _volumeInfoArrayModel = new ManagementListModel();
        _volumeList = new JList(_volumeInfoArrayModel);
        _volumeList.setPrototypeCellValue(PROTOTYPE_VOLUME_NAME);

        _treeInfoArrayModel = new ManagementTableModel(Management.TreeInfo.class, "TreeInfo", ui);

        _treeTable = new JTable(_treeInfoArrayModel);
        _treeTable.setAutoCreateRowSorter(true);
        _treeTable.setPreferredScrollableViewportSize(new Dimension(500, 50));
        _treeTable.setAutoCreateColumnsFromModel(false);
        _treeTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        _treeInfoArrayModel.formatColumns(_treeTable, null);

        _volumeList.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(final ListSelectionEvent lse) {
                final int index = _volumeList.getSelectedIndex();
                if (!lse.getValueIsAdjusting() && !_refreshing && index >= 0) {
                    final Management.VolumeInfo[] array = (Management.VolumeInfo[]) _volumeInfoArrayModel
                            .getInfoArray();
                    if (array != null && index < array.length) {
                        selectVolume(array[index]);
                    } else {
                        selectVolume(null);
                    }
                    _adminUI.scheduleRefresh(-1);
                }
            }
        });

        _treeTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(final ListSelectionEvent lse) {
                final int index = _treeTable.getSelectedRow();
                if (!lse.getValueIsAdjusting() && !_refreshing && index >= 0) {
                    boolean changed;
                    final Management.TreeInfo[] array = (Management.TreeInfo[]) _treeInfoArrayModel.getInfoArray();
                    if (array != null && index < array.length) {
                        changed = selectTree(array[index]);
                    } else {
                        changed = selectTree(null);
                    }
                    if (changed)
                        _adminUI.scheduleRefresh(-1);
                }
            }
        });

        _logicalRecordArrayModel = new ManagementSlidingTableModel(Management.LogicalRecord.class, "LogicalRecord", ui);

        _dataTable = new JTable(_logicalRecordArrayModel) {
            // Here we are trying to fool Mother Nature. In JTable's
            // implementation of tableRowsDeleted, there is a repaint(Rectangle)
            // operation called on the Component itself, and that causes
            // us to re-fetch every row. This takes forever.
            // A comment in the implementation of tableRowsDeleted says that
            // the repaint is needed only when the JTable is not contained
            // in a JScrollPane. So, this simply cancels the call to repaint
            // when we are deleting rows.
            //
            @Override
            public void repaint(final Rectangle drawRect) {
                if (!_logicalRecordArrayModel.isDeletingRows()) {
                    super.repaint(drawRect);
                }
            }
        };

        _dataTable.setCellSelectionEnabled(true);

        _dataTable.setPreferredScrollableViewportSize(new Dimension(600, 300));
        _dataTable.setAutoCreateColumnsFromModel(false);
        _dataTable.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
        _logicalRecordArrayModel.formatColumns(_dataTable, null);

        final JScrollPane volumeScrollPane = new JScrollPane(_volumeList);
        volumeScrollPane.setBorder(null);
        _volumePanel.setBorder(_adminUI.createTitledBorder("TreePanel.volumes"));
        _volumePanel.add(volumeScrollPane, BorderLayout.CENTER);

        final JScrollPane treeScrollPane = new JScrollPane(_treeTable);
        treeScrollPane.setBorder(null);
        _treePanel.setBorder(_adminUI.createTitledBorder("TreePanel.trees"));
        _treePanel.add(treeScrollPane, BorderLayout.CENTER);

        final JScrollPane dataScrollPane = new JScrollPane(_dataTable);
        dataScrollPane.setBorder(null);
        _dataPanel.setBorder(_adminUI.createTitledBorder("TreePanel.data"));
        _dataPanel.add(dataScrollPane, BorderLayout.CENTER);

        _inspectorPanel.setBorder(_adminUI.createTitledBorder("TreePanel.inspector"));

        final JSplitPane topSplitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        topSplitPane.setResizeWeight(TOP_SPLITPANE_RESIZE_WEIGHT);

        final JSplitPane bottomSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        bottomSplitPane.setResizeWeight(BOTTOM_SPLITPANE_RESIZE_WEIGHT);
        bottomSplitPane.setOneTouchExpandable(true);

        topSplitPane.add(_volumePanel);
        topSplitPane.add(_treePanel);
        bottomSplitPane.add(_dataPanel);
        bottomSplitPane.add(_inspectorPanel);
        final JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(topSplitPane, BorderLayout.CENTER);
        topPanel.add(createDataSelectorPanel(), BorderLayout.SOUTH);

        final JSplitPane mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        mainSplitPane.setResizeWeight(MAIN_SPLITPANE_RESIZE_WEIGHT);

        mainSplitPane.add(topPanel);
        mainSplitPane.add(bottomSplitPane);

        setLayout(new BorderLayout());

        _adminUI.registerTextComponent(_dataTable);
        _adminUI.registerTextComponent(_filterTextField);

        add(mainSplitPane, BorderLayout.CENTER);

        _adminUI.scheduleRefresh(-1);
    }

    JPanel createDataSelectorPanel() {
        final JPanel panel = new JPanel(new BorderLayout(3, 3));
        panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

        _displayAction = _adminUI.createAction(this, _adminUI.getProperty("TreePanel.display"));

        _displayButton = new JButton(_displayAction);
        _displayAction.addButton(_displayButton);

        _filterTextField = new JTextField();
        final JLabel label = new JLabel(_adminUI.getProperty("TreePanel.filterCaption"));
        final JPanel filterPanel = new JPanel(new BorderLayout());
        filterPanel.add(label, BorderLayout.WEST);
        filterPanel.add(_filterTextField, BorderLayout.CENTER);
        label.setFont(_adminUI.getBoldFont());
        label.setForeground(_adminUI.getPersistitAccentColor());
        panel.add(_displayButton, BorderLayout.WEST);
        panel.add(filterPanel, BorderLayout.CENTER);

        final JMenuItem displayMenuItem = new JMenuItem(_displayAction);
        final JMenuItem[] menus = new JMenuItem[] { displayMenuItem };
        _menuMap.put("VIEW.1", menus);
        _menuMap.put("VIEW.2", _adminUI.createMenuArray(_adminUI, "TreePanelMenu", "VIEW"));

        final ListSelectionListener listener = new ListSelectionListener() {
            @Override
            public void valueChanged(final ListSelectionEvent evt) {
                final int row = _dataTable.getSelectedRow();
                final int column = _dataTable.getSelectedColumn();
                if (row >= 0 && column >= 0) {
                    final Management.LogicalRecord lr = (Management.LogicalRecord) _logicalRecordArrayModel.getValueAt(
                            row, -1);
                    // null if "Waiting..."
                    if (lr != null) {
                        _inspectorPanel.setLogicalRecord(_selectedVolumeName, _selectedTreeName, lr);
                        // TODO
                        _inspectorPanel.setShowValue(column == 1);
                        _inspectorPanel.refreshed();
                    }
                }
            }
        };

        _dataTable.getSelectionModel().addListSelectionListener(listener);

        _dataTable.getColumnModel().getSelectionModel().addListSelectionListener(listener);

        return panel;
    }

    @Override
    public void actionPerformed(final AdminUI.AdminAction action, final ActionEvent ae) {
        final String name = action.getName();
        if ("DISPLAY".equals(name)) {
            _dataTable.scrollRectToVisible(new Rectangle(0, 0));

            _logicalRecordArrayModel.set(_selectedVolumeName, _selectedTreeName, _filterTextField.getText());
        }
    }

    private void selectVolume(final Management.VolumeInfo volumeInfo) {
        final String newName = volumeInfo == null ? null : volumeInfo.getName();
        if (!equals(newName, _selectedVolumeName)) {
            _logicalRecordArrayModel.setInfoArray(null);
            _selectedTreeName = null;
            _selectedVolumeName = newName;
        }
    }

    private boolean selectTree(final Management.TreeInfo treeInfo) {
        final String newName = treeInfo == null ? null : treeInfo.getName();
        if (!equals(newName, _selectedTreeName)) {
            _logicalRecordArrayModel.setInfoArray(null);
            _selectedTreeName = newName;
            return true;
        } else {
            return false;
        }
    }

    private boolean equals(final Object a, final Object b) {
        if (a == null)
            return b == null;
        else
            return a.equals(b);
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
                _selectedTreeName = null;
                _selectedVolumeName = null;
                _logicalRecordArrayModel.setInfoArray(null);
            }

            Management.VolumeInfo[] volumeInfoArray = null;
            if (management != null) {
                volumeInfoArray = management.getVolumeInfoArray();
            }
            _volumeInfoArrayModel.setInfoArray(volumeInfoArray);

            Management.TreeInfo[] treeInfoArray = null;
            if (management != null && _selectedVolumeName != null) {
                treeInfoArray = management.getTreeInfoArray(_selectedVolumeName);
            }
            _treeInfoArrayModel.setInfoArray(treeInfoArray);
            if (_selectedTreeName != null) {
                for (int index = 0; index < treeInfoArray.length; index++) {
                    if (_selectedTreeName.equals(treeInfoArray[index].getName())) {
                        _treeTable.getSelectionModel().setSelectionInterval(index, index);
                    }
                }
            }

            _displayAction.setEnabled(_selectedVolumeName != null && _selectedTreeName != null);

            _inspectorPanel.refresh(reset);
        } catch (final RemoteException re) {
            _adminUI.postException(re);
        } finally {
            synchronized (this) {
                _refreshing = false;
            }
        }
    }

    @Override
    protected Map getMenuMap() {
        return _menuMap;
    }

    @Override
    protected void setDefaultButton() {
        getRootPane().setDefaultButton(_displayButton);
    }
}
