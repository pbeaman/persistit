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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.Management.JournalInfo;
import com.persistit.Management.VolumeInfo;
import com.persistit.TimestampAllocator.Checkpoint;

/**
 * Task to perform and/or control hot (concurrent) backup. Generally the process
 * of copying large data files is best done by third party utility software.
 * This class provides built-in ability to copy database files, but production
 * configurations will likely use only the control mechanisms of this class and
 * will run external programs to perform data compression and copying.
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
 * form a complete backup, and as a convenience, a mechanism to compress and
 * copy them.
 * <p>
 * 
 * @author peter
 * 
 */
public class BackupTask extends Task {

    private final static int BUFFER_SIZE = 1024 * 1024;
    private final static int PROGRESS_MARK_AT = 100 * 1000 * 1000;
    private boolean _start;
    private boolean _end;
    private boolean _showFiles;
    private boolean _compressed;
    private boolean _checkpoint;
    private boolean _copyback;
    private String _toFile;
    final List<String> _files = new ArrayList<String>();
    private volatile String _backupStatus;

    @Cmd("backup")
    static Task setupTask(
            @Arg("file|string|Archive file path") String file,
            @Arg("_flag|a|Start appendOnly mode") boolean start,
            @Arg("_flag|e|End appendOnly mode") boolean end,
            @Arg("_flag|c|Request checkpoint before backup") boolean checkpoint,
            @Arg("_flag|z|Compress output to ZIP format") boolean compressed,
            @Arg("_flag|f|Emit a list of files that need to be copied") boolean showFiles,
            @Arg("_flag|y|Copyback pages before starting") boolean copyback)
            throws Exception {
        final BackupTask task = new BackupTask();
        task._toFile = file;
        task._start = start;
        task._end = end;
        task._showFiles = showFiles;
        task._compressed = compressed;
        task._checkpoint = checkpoint;
        task._copyback = copyback;
        return task;
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
        if (_checkpoint) {
            postMessage("Waiting for checkpoint", 0);
            final Checkpoint cp = _persistit.checkpoint();
            if (cp == null) {
                postMessage("Checkpoint failed", 0);
            } else {
                postMessage("Checkpoint " + cp + " written", 0);
            }
        }
        if (_copyback && !wasAppendOnly) {
            postMessage("Copying back pages from journal", 0);
            long start = _persistit.getJournalManager().getCopiedPageCount();
            _persistit.copyBackPages();
            long end = _persistit.getJournalManager().getCopiedPageCount();
            postMessage((end - start) + " pages copied", 0);
        }
        try {
            if (_showFiles || !_toFile.isEmpty()) {
                management.setAppendOnly(true);
                populateBackupFiles();
                if (!_toFile.isEmpty()) {
                    doBackup();
                }
            }
        } catch (Exception e) {
            _backupStatus = "Failed: " + e;
        } finally {
            management.setAppendOnly(_start ? true : _end ? false
                    : wasAppendOnly);
        }
    }

    protected void postMessage(final String message, int level) {
        super.postMessage(message, level);
        _backupStatus = message;
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
            _files.add(file.getAbsolutePath());
        }
        final StringBuilder sb = new StringBuilder();
        for (final String file : _files) {
            sb.append(file);
            sb.append(Util.NEW_LINE);
        }
        _backupStatus = sb.toString();
    }

    /**
     * A convenience method for backing up relatively small amounts of data from
     * within Persistit. In production it is expected that the actual file
     * copies required for backup will be performed be third-party utilities.
     * 
     * @throws Exception
     */
    private void doBackup() throws Exception {
        final ZipOutputStream zos = new ZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(_toFile),
                        BUFFER_SIZE));
        try {
            final byte[] buffer = new byte[65536];
            zos.setLevel(_compressed ? ZipOutputStream.DEFLATED
                    : ZipOutputStream.STORED);
            long size = 0;
            for (final String file : _files) {
                size += new File(file).length();
            }
            postMessage("Total size of files in backup set: "
                    + formatedSize(size), 0);
            for (final String path : _files) {
                final File file = new File(path);
                postMessage(
                        "Backing up " + path + " size="
                                + formatedSize(file.length()), 1);
                final ZipEntry ze = new ZipEntry(path);
                ze.setSize(file.length());
                ze.setTime(file.lastModified());
                zos.putNextEntry(ze);
                long progress = 0;
                long fileSize = 0;
                final BufferedInputStream is = new BufferedInputStream(
                        new FileInputStream(file), BUFFER_SIZE);
                try {
                    int readCount = 0;
                    while ((readCount = is.read(buffer, 0, buffer.length)) != -1) {
                        zos.write(buffer, 0, readCount);
                        progress += readCount;
                        fileSize += readCount;
                        if (progress > PROGRESS_MARK_AT) {
                            progress -= PROGRESS_MARK_AT;
                            appendMessage(" (" + formatedSize(fileSize) + ")",
                                    1);
                        }
                        poll();
                    }
                } finally {
                    is.close();
                }
            }
            postMessage("Backup of " + _files.size() + " files to " + _toFile
                    + " completed", 0);
        } finally {
            zos.close();
        }
    }

    /**
     * A convenience method for unit tests to unzip a backup created by
     * {@link #doBackup()}. In production it is expected a backup produced by
     * {@link #doBackup()} will be restored via an external unzip utility.
     * 
     * @throws Exception
     */
    public void doRestore(final String path) throws Exception {
        final File zipFile = new File(path);
        final byte[] buffer = new byte[65536];
        postMessage("Unzipping files from " + zipFile + " size="
                + formatedSize(zipFile.length()), 0);
        final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(
                new FileInputStream(zipFile), BUFFER_SIZE));
        ZipEntry ze;
        while ((ze = zis.getNextEntry()) != null) {
            postMessage("Unzipping " + ze, 0);
            final File file = new File(ze.getName());
            if (file.exists()) {
                rename(file);
            }
            long progress = 0;
            long fileSize = 0;
            final OutputStream os = new BufferedOutputStream(
                    new FileOutputStream(file, false));
            int writeCount = 0;
            while ((writeCount = zis.read(buffer)) != -1) {
                os.write(buffer, 0, writeCount);
                progress += writeCount;
                fileSize += writeCount;
                if (progress > PROGRESS_MARK_AT) {
                    progress -= PROGRESS_MARK_AT;
                    appendMessage(" " + formatedSize(fileSize), 1);
                }
            }
            os.close();
        }
        zis.close();
    }

    private void rename(final File file) throws Exception {
        for (int k = 0; k < 1000; k++) {
            final String candidate = k == 0 ? file.getAbsolutePath() + "~"
                    : file.getAbsoluteFile() + "~" + k;
            final File newFile = new File(candidate);
            if (!newFile.exists()) {
                file.renameTo(newFile);
                return;
            }
        }
        throw new IOException("Unable to rename file " + file);
    }

    private String formatedSize(final long size) {
        long value = size;
        int scale = 0;
        while (value > 9999) {
            value = (value + 499) / 1000;
            scale++;
        }
        return String.format("%,d", value)
                + " KMGTPE".substring(scale, scale + 1);
    }

    @Override
    public String getStatus() {
        return _backupStatus;
    }

    public List<String> getFileList() {
        return _files;
    }
}
