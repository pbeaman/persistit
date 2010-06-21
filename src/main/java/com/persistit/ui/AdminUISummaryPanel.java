/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Sep 6, 2004
 */
package com.persistit.ui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.persistit.Management;

/**
 * @version 1.0
 */
public class AdminUISummaryPanel extends AdminPanel implements AdminCommand {
    AdminUI _adminUI;

    JPanel _summaryPanel;
    JPanel _volumeTablePanel;

    JTextField _version;
    JTextField _copyright;
    JTextField _elapsed;
    JTextField _started;
    JTextField _totalReads;
    JTextField _totalWrites;
    JTextField _totalGets;
    JTextField _totalHits;
    JTextField _hitRatio;
    JTextField _frozenUpdates;
    JTextField _frozenShutdown;

    String _frozenTrueCaption = "FROZEN";
    String _frozenFalseCaption = "normal";

    Color _normalForegroundColor;

    JTable _volumeTable;
    ManagementTableModel _volumeInfoArrayModel;
    private Map _menuMap = new HashMap();
    private String _selectedVolumeName;

    protected void setup(AdminUI ui) throws NoSuchMethodException,
            RemoteException {
        _adminUI = ui;
        setLayout(new BorderLayout());
        _summaryPanel = new JPanel(new GridBagLayout());
        _volumeTablePanel = new JPanel(new BorderLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 1, 1, 5);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.BOTH;

        _version = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.version", false);
        _started = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.started", false);
        _elapsed = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.elapsed", true);

        _copyright = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.copyright", true);

        _totalWrites = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.writes", false);
        _totalReads = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.reads", false);
        _totalGets = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.gets", false);
        _hitRatio = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.ratio", true);

        _frozenUpdates = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.update_suspended", false);
        _frozenShutdown = (JTextField) ui.addLabeledField(_summaryPanel, gbc,
                new JTextField(), "SummaryPanel.shutdown_suspended", true);
        _frozenTrueCaption = _adminUI
                .getProperty("SummaryPanel.suspendedTrueCaption");
        _frozenFalseCaption = _adminUI
                .getProperty("SummaryPanel.suspendedFalseCaption");

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.WEST;
        add(new JLabel(_adminUI.getProperty("volumes")));

        _volumeInfoArrayModel = new ManagementTableModel(
                Management.VolumeInfo.class, "VolumeInfo", ui);

        final JTable volumeTable = new JTable(_volumeInfoArrayModel);
        volumeTable.setAutoCreateRowSorter(true);
        volumeTable.setPreferredScrollableViewportSize(new Dimension(800, 60));
        volumeTable.setAutoCreateColumnsFromModel(false);
        volumeTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        _volumeInfoArrayModel.formatColumns(volumeTable, null);

        volumeTable.getSelectionModel().addListSelectionListener(
                new ListSelectionListener() {
                    public void valueChanged(ListSelectionEvent lse) {
                        int index = volumeTable.getSelectedRow();
                        if (!lse.getValueIsAdjusting() && index >= 0) {
                            Management.VolumeInfo[] array = (Management.VolumeInfo[]) _volumeInfoArrayModel
                                    .getInfoArray();
                            if (array != null && index < array.length) {
                                _selectedVolumeName = array[index].getPath();
                            } else {
                                _selectedVolumeName = null;
                            }
                            _adminUI.scheduleRefresh(-1);
                        }
                    }
                });

        JScrollPane volumeScrollPane = new JScrollPane(volumeTable);

        JPanel volumePanel = new JPanel(new BorderLayout());
        volumePanel.setBorder(_adminUI
                .createTitledBorder("SummaryPanel.volumes"));
        volumePanel.add(volumeScrollPane, BorderLayout.CENTER);

        JSplitPane splitter1 = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitter1.add(volumePanel);

        _volumeTablePanel.add(splitter1, BorderLayout.CENTER);

        gbc.gridx = 0;
        gbc.gridy = 0;

        _normalForegroundColor = _frozenUpdates.getForeground();

        _summaryPanel.setBorder(_adminUI
                .createTitledBorder("SummaryPanel.status"));

        add(_summaryPanel, BorderLayout.NORTH);
        add(_volumeTablePanel, BorderLayout.CENTER);

        refresh(false);
    }

    public void actionPerformed(AdminUI.AdminAction action, ActionEvent ae) {
        //
        // Currently no actions created by this panel
        //
    }

    protected void refresh(boolean reset) {
        try {
            Management management = _adminUI.getManagement();
            if (management == null) {
                _version.setText("");
                _copyright.setText("");
                _started.setText("");
                _elapsed.setText("");

                _totalReads.setText("");
                _totalWrites.setText("");
                _totalGets.setText("");
                _hitRatio.setText("");
                _volumeInfoArrayModel.setInfoArray(null);
            } else {
                _version.setText(management.getVersion());
                _copyright.setText(management.getCopyright());

                _started
                        .setText(_adminUI.formatDate(management.getStartTime()));

                _elapsed.setText(_adminUI.formatTime(management
                        .getElapsedTime()));

                _frozenUpdates
                        .setText(management.isUpdateSuspended() ? _frozenTrueCaption
                                : _frozenFalseCaption);

                _frozenUpdates
                        .setForeground(management.isUpdateSuspended() ? Color.red
                                : _normalForegroundColor);

                _frozenShutdown
                        .setText(management.isShutdownSuspended() ? _frozenTrueCaption
                                : _frozenFalseCaption);

                _frozenShutdown
                        .setForeground(management.isShutdownSuspended() ? Color.red
                                : _normalForegroundColor);

                Management.BufferPoolInfo[] bpia = management
                        .getBufferPoolInfoArray();

                long reads = 0;
                long writes = 0;
                long gets = 0;
                long hits = 0;

                for (int index = 0; index < bpia.length; index++) {
                    gets += bpia[index].getGetCounter();
                    hits += bpia[index].getHitCounter();
                }

                Management.VolumeInfo[] via = management.getVolumeInfoArray();

                if (_selectedVolumeName == null && via.length > 0) {
                    _selectedVolumeName = via[0].getPath();
                }

                for (int index = 0; index < via.length; index++) {
                    reads += via[index].getReadCounter();
                    writes += via[index].getWriteCounter();
                }

                _totalReads.setText(_adminUI.formatLong(reads));
                _totalWrites.setText(_adminUI.formatLong(writes));
                _totalGets.setText(_adminUI.formatLong(gets));
                _hitRatio.setText(gets > 0 ? _adminUI
                        .formatPercent((double) hits / (double) gets) : "n/a");
                _volumeInfoArrayModel.setInfoArray(via);
            }
        } catch (RemoteException re) {
            _adminUI.postException(re);
        }
    }

    protected Map getMenuMap() {
        return _menuMap;
    }

    protected void setDefaultButton() {
        getRootPane().setDefaultButton(null);
    }

}
