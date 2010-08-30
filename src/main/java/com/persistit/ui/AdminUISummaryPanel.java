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
    JPanel _journalPanel;
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

    JTextField _journalCurLocation;
    JTextField _journalAddressMax;

    JTextField _journalGenerationRange;
    JTextField _journalPageMapSize;
    JTextField _journalPageCount;
    JTextField _journalCopiedPageCount;

    JTextField _journalRecoveryStatus;
    JTextField _journalRecoveryLocation;

    JTextField _journalValidCkptTime;
    JTextField _journalValidCkptAgo;
    JTextField _journalValidCkptTimestamp;
    JTextField _journalValidCkptLocation;

    JTextField _journalCopyingFrozen;

    JTextField _frozenUpdates;
    JTextField _frozenShutdown;

    String _frozenTrueCaption;
    String _frozenFalseCaption;
    String _dirtyCaption;
    String _cleanCaption;

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
        _journalPanel = new JPanel(new GridBagLayout());
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

        _journalCurLocation = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_current_location",
                false);
        _journalGenerationRange = (JTextField) ui.addLabeledField(
                _journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_generation_range", false);
        _journalAddressMax = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_max_file_length",
                true);

        _journalPageMapSize = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_page_map_size",
                false);
        _journalPageCount = (JTextField) ui.addLabeledField(_journalPanel, gbc,
                new JTextField(), "JournalPanel.journal_page_count", false);
        _journalCopiedPageCount = (JTextField) ui.addLabeledField(
                _journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_copied_page_count", true);

        _journalRecoveryLocation = (JTextField) ui.addLabeledField(
                _journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_recovery_location", false);
        _journalRecoveryStatus = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_recovery_status",
                true);

        _journalValidCkptLocation = (JTextField) ui.addLabeledField(
                _journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_location", false);
        _journalValidCkptTimestamp = (JTextField) ui.addLabeledField(
                _journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_timestamp", true);

        _journalValidCkptTime = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_ckpt_time", false);
        _journalValidCkptAgo = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(), "JournalPanel.journal_ckpt_ago", true);

        _journalCopyingFrozen = (JTextField) ui.addLabeledField(_journalPanel,
                gbc, new JTextField(),
                "JournalPanel.journal_copying_suspended", true);

        _frozenTrueCaption = _adminUI
                .getProperty("SummaryPanel.suspendedTrueCaption");
        _frozenFalseCaption = _adminUI
                .getProperty("SummaryPanel.suspendedFalseCaption");
        _dirtyCaption = _adminUI.getProperty("SummaryPanel.dirtyCaption");
        _cleanCaption = _adminUI.getProperty("SummaryPanel.cleanCaption");

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

        _journalPanel.setBorder(_adminUI
                .createTitledBorder("JournalPanel.journal_status"));

        JPanel panel = new JPanel(new BorderLayout());
        panel.add(_summaryPanel, BorderLayout.NORTH);
        panel.add(_journalPanel, BorderLayout.SOUTH);
        add(panel, BorderLayout.NORTH);
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

                _journalCurLocation.setText("");
                _journalAddressMax.setText("");

                _journalGenerationRange.setText("");
                _journalPageMapSize.setText("");
                _journalPageCount.setText("");
                _journalCopiedPageCount.setText("");

                _journalRecoveryStatus.setText("");
                _journalRecoveryLocation.setText("");

                _journalValidCkptTime.setText("");
                _journalValidCkptAgo.setText("");
                _journalValidCkptTimestamp.setText("");
                _journalValidCkptLocation.setText("");

                _journalCopyingFrozen.setText("");

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

                final Management.JournalInfo jinfo = management
                        .getJournalInfo();
                _journalCurLocation.setText(_adminUI.formatFileLocation(jinfo
                        .getCurrentJournalFile(), jinfo
                        .getCurrentJournalAddress()));
                _journalAddressMax.setText(_adminUI.formatLong(jinfo
                        .getMaxJournalFileSize()));

                _journalGenerationRange.setText(_adminUI.formatLong(jinfo
                        .getStartGeneration())
                        + " - "
                        + _adminUI.formatLong(jinfo.getCurrentGeneration()));

                _journalPageMapSize.setText(_adminUI.formatLong(jinfo
                        .getPageMapSize()));
                _journalPageCount.setText(_adminUI.formatLong(jinfo
                        .getJournaledPageCount()));
                _journalCopiedPageCount.setText(_adminUI.formatLong(jinfo
                        .getCopiedPageCount()));

                _journalRecoveryStatus.setText(formatRecoveryStatus(jinfo
                        .getRecoveryStatus()));
                _journalRecoveryLocation.setText(_adminUI.formatFileLocation(
                        jinfo.getRecoveryJournalFile(), jinfo
                                .getRecoveryJournalAddress()));

                if (jinfo.getLastValidCheckpointSystemTime() != 0) {
                    _journalValidCkptTime.setText(_adminUI.formatDate(jinfo
                            .getLastValidCheckpointSystemTime()));
                    _journalValidCkptAgo.setText(_adminUI.formatLong(jinfo
                            .getLastValidCheckpointAge()));
                    _journalValidCkptTimestamp
                            .setText(_adminUI.formatLong(jinfo
                                    .getLastValidCheckpointTimestamp()));
                    _journalValidCkptLocation.setText(_adminUI
                            .formatFileLocation(jinfo
                                    .getLastValidCheckpointJournalFile(), jinfo
                                    .getLastValidCheckpointJournalAddress()));
                }

                _journalCopyingFrozen
                        .setText(jinfo.isSuspendCopying() ? _frozenTrueCaption
                                : _frozenFalseCaption);

                final Management.BufferPoolInfo[] bpia = management
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

    private String formatRecoveryStatus(final long status) {
        if (status == Long.MIN_VALUE) {
            return "";
        }
        if (status == -1) {
            return _dirtyCaption;
        }
        if (status == 0) {
            return _cleanCaption;
        }
        return _adminUI.formatLong(status);
    }

    protected Map getMenuMap() {
        return _menuMap;
    }

    protected void setDefaultButton() {
        getRootPane().setDefaultButton(null);
    }

}
