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

package com.persistit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.persistit.Management.JournalInfo;
import com.persistit.Management.VolumeInfo;

/**
 * Tasks to perform and/or control hot (concurrent) backup. Generally the
 * process of copying large data files is best done by third party utility
 * software. This class provides built-in ability to copy database files, but
 * generally customers will use only the control mechanisms of this class and
 * will run externally-supplied programs to perform data compression and
 * copying.
 * <p>
 * To perform a concurrent backup, Persistit needs to ensure that the state of
 * copied volume files plus journal files is sufficient to ensure a consistent,
 * fast recovery process. This is done by setting Persistit to
 * <code>appendOnly</code> mode, which causes Persistit not to modify its Volume
 * files and not to delete any journal files. A snapshot of the journal file and
 * volume provides a reliable source for recovery.
 * <p>
 * This class provides a {@link Task} implementation that sets and resets the
 * <code>appendOnly</code> flag, provides a list a files needing to be copied to
 * form a complete backup, and as a convenience, a mechanism to copy them.
 * <p>
 * 
 * @author peter
 * 
 */
public class Backup extends Task {

    static final String COMMAND_NAME = "backup";
    static final String[] ARG_TEMPLATE = new String[] {
            "_flag|a|Start appendOnly mode", "_flag|e|End appendOnly mode",
            "_flag|z|Compress output to ZIP format",
            "_flag|f|Emit a list of files that need to be copied",
            "file|string|Archive file path",

    };

    private boolean _start;
    private boolean _end;
    private boolean _showFiles;
    private boolean _compressed;
    private String _toFile;
    final List<String> _files = new ArrayList<String>();
    private String _backupStatus;

    @Override
    public boolean isImmediate() {
        return _toFile == null || _toFile.isEmpty();
    }

    @Override
    protected void setupArgs(String[] args) throws Exception {
        _toFile = args.length > 0 ? args[0] : null;
        String flags = args.length > 1 ? args[1] : "";

        _start = flags.indexOf('a') >= 0;
        _end = flags.indexOf('e') >= 0;
        _showFiles = flags.indexOf('f') >= 0;
        _compressed = flags.indexOf('z') >= 0;
    }

    @Override
    protected void setupTask(ArgParser ap) throws Exception {
        _start = ap.isFlag('a');
        _end = ap.isFlag('e');
        _showFiles = ap.isFlag('f');
        _compressed = ap.isFlag('z');
        _toFile = ap.getStringValue("file");
    }

    private void validate() {
        if (_toFile == null) {
            _toFile = "";
        }
    }

    @Override
    protected void runTask() throws Exception {
        validate();
        final Management management = _persistit.getManagement();
        boolean wasAppendOnly = management.getJournalInfo().isAppendOnly();
        if (_start || !_toFile.isEmpty()) {
            management.setAppendOnly(true);
        }
        if (_showFiles || !_toFile.isEmpty()) {
            populateBackupFiles();
        }
        if (!_toFile.isEmpty()) {
            doBackup();
        }
        if (!_toFile.isEmpty()) {
            management.setAppendOnly(wasAppendOnly);
        }
        if (_end) {
            management.setAppendOnly(false);
        }

    }

    private void populateBackupFiles() throws Exception {
        final VolumeInfo[] volumes = _persistit.getManagement()
                .getVolumeInfoArray();
        for (final VolumeInfo info : volumes) {
            if (!info.isTransient()) {
                _files.add(info.getPath());
            }
        }
        final JournalInfo info = _persistit.getManagement().getJournalInfo();
        final long baseAddress = info.getBaseAddress();
        final long currentAddress = info.getCurrentJournalAddress();
        final long blockSize = info.getBlockSize();
        String path = JournalManager.fileToPath(new File(info
                .getCurrentJournalFile()));
        for (long generation = baseAddress / blockSize; generation <= currentAddress
                / blockSize; generation++) {
            File file = JournalManager.generationToFile(path, generation);
            _files.add(file.getPath());
        }
    }

    private void doBackup() {
        System.out.println("Do backup of " + _files + " to " + _toFile);
    }

    @Override
    public String getStatus() {
        try {
            if (isImmediate()) {
                final StringBuilder sb = new StringBuilder("appendOnly="
                        + _persistit.getManagement().getJournalInfo()
                                .isAppendOnly());
                if (_showFiles) {
                    for (final String file : _files) {
                        sb.append(Util.NEW_LINE);
                        sb.append(file);
                    }
                }
                return sb.toString();
            } else {
                return _backupStatus;
            }
        } catch (Exception e) {
            return "Failed: " + e.toString();
        }
    }
}
