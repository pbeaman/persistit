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
    JPanel _recoveryPanel;
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

    JTextField _commits;
    JTextField _rollbacks;
    JTextField _rollbacksSinceLastCommit;

    JTextField _journalCurLocation;
    JTextField _journalBlockSize;

    JTextField _journalBaseAddress;
    JTextField _journalPageMapSize;
    JTextField _journalPageCount;
    JTextField _journalCopiedPageCount;
    JTextField _journalReadPageCount;

    JTextField _journalValidCkptTime;
    JTextField _journalValidCkptAgo;
    JTextField _journalValidCkptTimestamp;
    JTextField _journalValidCkptLocation;

    JTextField _journalAppendOnly;
    JTextField _journalCopyFast;

    JTextField _frozenUpdates;
    JTextField _frozenShutdown;

    JTextField _recoveryKeystone;
    JTextField _recoveryBaseAddress;
    JTextField _recoveryEndAddress;
    JTextField _recoveryCkptTime;
    JTextField _recoveryCkptTimestamp;
    JTextField _recoveryCkptLocation;
    JTextField _recoveryCommitted;
    JTextField _recoveryUncommitted;
    JTextField _recoveryApplied;
    JTextField _recoveryException;

    JTextField _journalRecoveryLocation;

    String _frozenTrueCaption;
    String _frozenFalseCaption;
    String _enabledCaption;
    String _disabledCaption;
    String _dirtyCaption;
    String _cleanCaption;

    Color _normalForegroundColor;

    JTable _volumeTable;
    ManagementTableModel _volumeInfoArrayModel;
    private final Map _menuMap = new HashMap();
    private String _selectedVolumeName;

    @Override
    protected void setup(final AdminUI ui) throws NoSuchMethodException, RemoteException {
        _adminUI = ui;
        setLayout(new BorderLayout());
        _summaryPanel = new JPanel(new GridBagLayout());
        _journalPanel = new JPanel(new GridBagLayout());
        _recoveryPanel = new JPanel(new GridBagLayout());
        _volumeTablePanel = new JPanel(new BorderLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 1, 1, 5);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.BOTH;

        _version = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.version", false);
        _started = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.started", false);
        _elapsed = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.elapsed", true);

        _copyright = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.copyright",
                true);

        _totalWrites = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.writes",
                false);
        _totalReads = (JTextField) ui
                .addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.reads", false);
        _totalGets = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.gets", false);
        _hitRatio = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.ratio", true);

        _commits = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.commits", false);
        _rollbacks = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(), "SummaryPanel.rollbacks",
                false);
        _rollbacksSinceLastCommit = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(),
                "SummaryPanel.rollbacksSinceLastCommit", true);

        _frozenUpdates = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(),
                "SummaryPanel.update_suspended", false);
        _frozenShutdown = (JTextField) ui.addLabeledField(_summaryPanel, gbc, new JTextField(),
                "SummaryPanel.shutdown_suspended", true);

        _journalCurLocation = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_current_location", false);
        _journalBaseAddress = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_base_address", false);
        _journalBlockSize = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_block_size", true);

        _journalPageMapSize = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_page_map_size", false);
        _journalPageCount = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_page_count", false);

        _journalCopiedPageCount = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_copy_count", false);

        _journalReadPageCount = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_read_count", true);

        _journalValidCkptLocation = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_location", false);

        _journalValidCkptTimestamp = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_timestamp", true);

        _journalValidCkptTime = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_time", false);

        _journalValidCkptAgo = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_ckpt_ago", true);

        _journalAppendOnly = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_append_only", false);

        _journalCopyFast = (JTextField) ui.addLabeledField(_journalPanel, gbc, new JTextField(),
                "JournalPanel.journal_copy_fast", true);

        _recoveryKeystone = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_location", false);

        _recoveryBaseAddress = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_base", false);

        _recoveryEndAddress = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_ended", true);

        _recoveryCkptLocation = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_ckpt_location", false);

        _recoveryCkptTimestamp = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_ckpt_timestamp", false);

        _recoveryCkptTime = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_ckpt_time", true);

        _recoveryCommitted = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_committed", false);

        _recoveryUncommitted = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_uncommitted", false);

        _recoveryApplied = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_applied", true);

        _recoveryException = (JTextField) ui.addLabeledField(_recoveryPanel, gbc, new JTextField(),
                "RecoveryPanel.recovery_exception", false);

        _frozenTrueCaption = _adminUI.getProperty("SummaryPanel.suspendedTrueCaption");
        _frozenFalseCaption = _adminUI.getProperty("SummaryPanel.suspendedFalseCaption");

        _enabledCaption = _adminUI.getProperty("SummaryPanel.enabledCaption");
        _disabledCaption = _adminUI.getProperty("SummaryPanel.disabledCaption");

        _dirtyCaption = _adminUI.getProperty("SummaryPanel.dirtyCaption");
        _cleanCaption = _adminUI.getProperty("SummaryPanel.cleanCaption");

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.WEST;
        add(new JLabel(_adminUI.getProperty("volumes")));

        _volumeInfoArrayModel = new ManagementTableModel(Management.VolumeInfo.class, "VolumeInfo", ui);

        final JTable volumeTable = new JTable(_volumeInfoArrayModel);
        volumeTable.setAutoCreateRowSorter(true);
        volumeTable.setPreferredScrollableViewportSize(new Dimension(800, 60));
        volumeTable.setAutoCreateColumnsFromModel(false);
        volumeTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        _volumeInfoArrayModel.formatColumns(volumeTable, null);

        volumeTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(final ListSelectionEvent lse) {
                final int index = volumeTable.getSelectedRow();
                if (!lse.getValueIsAdjusting() && index >= 0) {
                    final Management.VolumeInfo[] array = (Management.VolumeInfo[]) _volumeInfoArrayModel
                            .getInfoArray();
                    if (array != null && index < array.length) {
                        _selectedVolumeName = array[index].getName();
                    } else {
                        _selectedVolumeName = null;
                    }
                    _adminUI.scheduleRefresh(-1);
                }
            }
        });

        final JScrollPane volumeScrollPane = new JScrollPane(volumeTable);

        final JPanel volumePanel = new JPanel(new BorderLayout());
        volumePanel.setBorder(_adminUI.createTitledBorder("SummaryPanel.volumes"));
        volumePanel.add(volumeScrollPane, BorderLayout.CENTER);

        final JSplitPane splitter1 = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitter1.add(volumePanel);

        _volumeTablePanel.add(splitter1, BorderLayout.CENTER);

        gbc.gridx = 0;
        gbc.gridy = 0;

        _normalForegroundColor = _frozenUpdates.getForeground();

        _summaryPanel.setBorder(_adminUI.createTitledBorder("SummaryPanel.status"));

        _journalPanel.setBorder(_adminUI.createTitledBorder("JournalPanel.journal_status"));

        _recoveryPanel.setBorder(_adminUI.createTitledBorder("RecoveryPanel.recovery_status"));

        final JPanel panel = new JPanel(new GridBagLayout());
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(_summaryPanel, gbc);
        gbc.gridy++;
        panel.add(_journalPanel, gbc);
        gbc.gridy++;
        panel.add(_recoveryPanel, gbc);

        add(panel, BorderLayout.NORTH);
        add(_volumeTablePanel, BorderLayout.CENTER);

        refresh(false);
    }

    @Override
    public void actionPerformed(final AdminUI.AdminAction action, final ActionEvent ae) {
        //
        // Currently no actions created by this panel
        //
    }

    @Override
    protected void refresh(final boolean reset) {
        try {
            final Management management = _adminUI.getManagement();
            if (management == null) {
                _version.setText("");
                _copyright.setText("");
                _started.setText("");
                _elapsed.setText("");

                _totalReads.setText("");
                _totalWrites.setText("");
                _totalGets.setText("");
                _hitRatio.setText("");

                _commits.setText("");
                _rollbacks.setText("");
                _rollbacksSinceLastCommit.setText("");

                _journalCurLocation.setText("");
                _journalBlockSize.setText("");

                _journalBaseAddress.setText("");
                _journalPageMapSize.setText("");
                _journalPageCount.setText("");
                _journalCopiedPageCount.setText("");
                _journalReadPageCount.setText("");

                _journalValidCkptTime.setText("");
                _journalValidCkptAgo.setText("");
                _journalValidCkptTimestamp.setText("");
                _journalValidCkptLocation.setText("");

                _journalAppendOnly.setText("");
                _journalCopyFast.setText("");

                _recoveryApplied.setText("");
                _recoveryBaseAddress.setText("");
                _recoveryCkptLocation.setText("");
                _recoveryCkptTime.setText("");
                _recoveryCkptTimestamp.setText("");
                _recoveryCommitted.setText("");
                _recoveryEndAddress.setText("");
                _recoveryException.setText("");
                _recoveryKeystone.setText("");
                _recoveryUncommitted.setText("");

                _volumeInfoArrayModel.setInfoArray(null);

            } else {
                _version.setText(management.getVersion());
                _copyright.setText(management.getCopyright());

                _started.setText(_adminUI.formatDate(management.getStartTime()));

                _elapsed.setText(_adminUI.formatTime(management.getElapsedTime()));

                _frozenUpdates.setText(management.isUpdateSuspended() ? _frozenTrueCaption : _frozenFalseCaption);

                _frozenUpdates.setForeground(management.isUpdateSuspended() ? Color.red : _normalForegroundColor);

                _frozenShutdown.setText(management.isShutdownSuspended() ? _frozenTrueCaption : _frozenFalseCaption);

                _frozenShutdown.setForeground(management.isShutdownSuspended() ? Color.red : _normalForegroundColor);

                final Management.JournalInfo jinfo = management.getJournalInfo();
                _journalCurLocation.setText(_adminUI.formatFileLocation(jinfo.getCurrentJournalFile(),
                        jinfo.getCurrentJournalAddress()));
                _journalBlockSize.setText(_adminUI.formatLong(jinfo.getBlockSize()));

                _journalBaseAddress.setText(_adminUI.formatLong(jinfo.getBaseAddress()));

                _journalPageMapSize.setText(_adminUI.formatLong(jinfo.getPageMapSize()));
                _journalPageCount.setText(_adminUI.formatLong(jinfo.getJournaledPageCount()));
                _journalCopiedPageCount.setText(_adminUI.formatLong(jinfo.getCopiedPageCount()));
                _journalReadPageCount.setText(_adminUI.formatLong(jinfo.getReadPageCount()));

                if (jinfo.getLastValidCheckpointSystemTime() != 0) {
                    _journalValidCkptTime.setText(_adminUI.formatDate(jinfo.getLastValidCheckpointSystemTime()));
                    _journalValidCkptAgo.setText(_adminUI.formatLong(jinfo.getLastValidCheckpointAge()));
                    _journalValidCkptTimestamp.setText(_adminUI.formatLong(jinfo.getLastValidCheckpointTimestamp()));
                    _journalValidCkptLocation.setText(_adminUI.formatFileLocation(
                            jinfo.getLastValidCheckpointJournalFile(), jinfo.getLastValidCheckpointJournalAddress()));
                }

                _journalAppendOnly.setText(jinfo.isAppendOnly() ? _enabledCaption : _disabledCaption);

                _journalCopyFast.setText(jinfo.isFastCopying() ? _enabledCaption : _disabledCaption);

                final Management.RecoveryInfo rinfo = management.getRecoveryInfo();

                _recoveryApplied.setText(_adminUI.formatInteger(rinfo.getAppliedTransactions()));
                _recoveryBaseAddress.setText(_adminUI.formatLong(rinfo.getBaseAddress()));
                _recoveryCkptLocation.setText(_adminUI.formatLong(rinfo.getLastValidCheckpointJournalAddress()));
                _recoveryCkptTime.setText(_adminUI.formatDate(rinfo.getLastValidCheckpointSystemTime()));
                _recoveryCkptTimestamp.setText(_adminUI.formatLong(rinfo.getLastValidCheckpointTimestamp()));
                _recoveryCommitted.setText(_adminUI.formatInteger(rinfo.getCommittedTransactions()));
                _recoveryEndAddress.setText(_adminUI.formatLong(rinfo.getRecoveryEndAddress()));
                _recoveryException.setText(rinfo.getRecoveryEndedException() == null ? "none" : rinfo
                        .getRecoveryEndedException());
                _recoveryKeystone.setText(_adminUI.formatFileLocation(rinfo.getKeystoneJournalFile(),
                        rinfo.getKeystoneJournalAddress()));
                _recoveryUncommitted.setText(_adminUI.formatInteger(rinfo.getUncommittedTransactions()));

                final Management.BufferPoolInfo[] bpia = management.getBufferPoolInfoArray();

                long reads = 0;
                long writes = 0;
                long misses = 0;
                long hits = 0;
                long creates = 0;

                for (int index = 0; index < bpia.length; index++) {
                    misses += bpia[index].getMissCount();
                    hits += bpia[index].getHitCount();
                    creates += bpia[index].getNewCount();
                }

                final Management.VolumeInfo[] via = management.getVolumeInfoArray();

                if (_selectedVolumeName == null && via.length > 0) {
                    _selectedVolumeName = via[0].getName();
                }

                for (int index = 0; index < via.length; index++) {
                    reads += via[index].getReadCounter();
                    writes += via[index].getWriteCounter();
                }

                _totalReads.setText(_adminUI.formatLong(reads));
                _totalWrites.setText(_adminUI.formatLong(writes));
                _totalGets.setText(_adminUI.formatLong(misses));
                _hitRatio.setText(misses + hits + creates > 0 ? _adminUI.formatPercent((double) hits
                        / (double) (misses + hits + creates)) : "n/a");
                _volumeInfoArrayModel.setInfoArray(via);

                final Management.TransactionInfo transactionInfo = management.getTransactionInfo();
                _commits.setText(_adminUI.formatLong(transactionInfo.getCommitCount()));
                _rollbacks.setText(_adminUI.formatLong(transactionInfo.getRollbackCount()));
                _rollbacksSinceLastCommit.setText(_adminUI.formatLong(transactionInfo.getRollbackSinceCommitCount()));

            }
        } catch (final RemoteException re) {
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

    @Override
    protected Map getMenuMap() {
        return _menuMap;
    }

    @Override
    protected void setDefaultButton() {
        getRootPane().setDefaultButton(null);
    }

}
