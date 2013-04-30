/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * A volume header represents the header information at the beginning of each
 * volume file.
 */
class VolumeHeader {

    private final static String[] ARGS_TEMPLATE = { "path|string:|Volume file name" };

    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    /**
     * 
     * Signature value - human and machine readable confirmation that this file
     * resulted from Persistit.
     */
    private final static byte[] SIGNATURE = Util.stringToBytes("PERSISTIT VOLUME");
    /**
     * Minimum possible size for a volume
     */
    private final static int SIZE = Buffer.MIN_BUFFER_SIZE;
    /**
     * Current product version number.
     */
    private final static int CURRENT_VERSION = 221;

    /**
     * Minimum product version that can handle Volumes created by this version.
     */
    private final static int MIN_SUPPORTED_VERSION = 210;

    /**
     * Minimum product version that can handle Volumes created by this version.
     */
    private final static int MAX_SUPPORTED_VERSION = 299;

    static boolean verifySignature(final byte[] bytes) {
        return Util.bytesEqual(bytes, 0, SIGNATURE);
    }

    static boolean putSignature(final byte[] bytes) {
        return Util.changeBytes(bytes, 0, SIGNATURE);
    }

    static int getVersion(final byte[] bytes) {
        return Util.getInt(bytes, 16);
    }

    static void putVersion(final byte[] bytes) {
        Util.putInt(bytes, 16, CURRENT_VERSION);
    }

    static int getPageSize(final byte[] bytes) {
        return Util.getInt(bytes, 20);
    }

    static void putPageSize(final byte[] bytes, final int value) {
        Util.putInt(bytes, 20, value);
    }

    static long getTimestamp(final byte[] bytes) {
        return Util.getLong(bytes, 24);
    }

    static boolean changeTimestamp(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 24, value);
    }

    static long getId(final byte[] bytes) {
        return Util.getLong(bytes, 32);
    }

    static void putId(final byte[] bytes, final long value) {
        Util.putLong(bytes, 32, value);
    }

    static long getReadCounter(final byte[] bytes) {
        return Util.getLong(bytes, 40);
    }

    static boolean changeReadCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 40, value);
    }

    static long getWriteCounter(final byte[] bytes) {
        return Util.getLong(bytes, 48);
    }

    static boolean changeWriteCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 48, value);
    }

    static long getGetCounter(final byte[] bytes) {
        return Util.getLong(bytes, 56);
    }

    static boolean changeGetCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 56, value);
    }

    static long getOpenTime(final byte[] bytes) {
        return Util.getLong(bytes, 64);
    }

    static boolean changeOpenTime(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 64, value);
    }

    static long getCreateTime(final byte[] bytes) {
        return Util.getLong(bytes, 72);
    }

    static boolean changeCreateTime(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 72, value);
    }

    static long getLastReadTime(final byte[] bytes) {
        return Util.getLong(bytes, 80);
    }

    static boolean changeLastReadTime(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 80, value);
    }

    static long getLastWriteTime(final byte[] bytes) {
        return Util.getLong(bytes, 88);
    }

    static boolean changeLastWriteTime(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 88, value);
    }

    static long getLastExtensionTime(final byte[] bytes) {
        return Util.getLong(bytes, 96);
    }

    static boolean changeLastExtensionTime(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 96, value);
    }

    /**
     * +1 because the stored form of volumes created before 2.6.1 recorded
     * "highestUsedPage" rather than "nextAvailablePage" in this slot.
     * 
     * @param bytes
     * @return page address of the next available page
     */
    static long getNextAvailablePage(final byte[] bytes) {
        return Util.getLong(bytes, 104) + 1;
    }

    /**
     * -1 because the stored form of volumes created before 2.6.1 recorded
     * "highestUsedPage" rather than "nextAvailablePage" in this slot.
     * 
     * @param bytes
     * @param value
     *            next available page
     * @return whether the supplied value is different from the previously
     *         stored value
     */
    static boolean changeNextAvailablePage(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 104, value - 1);
    }

    static long getExtendedPageCount(final byte[] bytes) {
        return Util.getLong(bytes, 112);
    }

    static boolean changeExtendedPageCount(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 112, value);
    }

    static long getExtensionPages(final byte[] bytes) {
        return Util.getLong(bytes, 120);
    }

    static boolean changeExtensionPages(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 120, value);
    }

    static long getMaximumPages(final byte[] bytes) {
        return Util.getLong(bytes, 128);
    }

    static boolean changeMaximumPages(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 128, value);
    }

    static long getDirectoryRoot(final byte[] bytes) {
        return Util.getLong(bytes, 144);
    }

    static boolean changeDirectoryRoot(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 144, value);
    }

    static long getGarbageRoot(final byte[] bytes) {
        return Util.getLong(bytes, 152);
    }

    static boolean changeGarbageRoot(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 152, value);
    }

    static long getfetchCounter(final byte[] bytes) {
        return Util.getLong(bytes, 160);
    }

    static boolean changeFetchCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 160, value);
    }

    static long getTraverseCounter(final byte[] bytes) {
        return Util.getLong(bytes, 168);
    }

    static boolean changeTraverseCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 168, value);
    }

    static long getStoreCounter(final byte[] bytes) {
        return Util.getLong(bytes, 176);
    }

    static boolean changeStoreCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 176, value);
    }

    static long getRemoveCounter(final byte[] bytes) {
        return Util.getLong(bytes, 184);
    }

    static boolean changeRemoveCounter(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 184, value);
    }

    static long getInitialPages(final byte[] bytes) {
        return Util.getLong(bytes, 192);
    }

    static boolean changeInitialPages(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 192, value);
    }

    static long getGlobalTimestamp(final byte[] bytes) {
        return Util.getLong(bytes, 200);
    }

    static boolean changeGlobalTimestamp(final byte[] bytes, final long value) {
        return Util.changeLong(bytes, 200, value);
    }

    /**
     * Validate that the header conforms to the volume header specification and
     * if so, read the pageSize and version values from it and populate the
     * corresponding fields of the supplied <code>VolumeSpecification</code>.
     * CorruptVolumeExceptions are thrown when an inconsistency is observed.
     * 
     * @return <code>true</code> if and only if there already exists a Volume
     *         file.
     * @throws PersistitException
     * @throws IOException
     * @throws InvalidVolumeSpecificationException
     * @throws CorruptVolumeException
     * @throws PersistitIOException
     */
    public static boolean verifyVolumeHeader(final VolumeSpecification specification, final long systemTimestamp)
            throws CorruptVolumeException, InvalidVolumeSpecificationException, PersistitIOException {
        FileInputStream stream = null;
        try {
            final File file = new File(specification.getPath());
            if (file.exists()) {
                if (file.isFile()) {
                    stream = new FileInputStream(file);
                    final byte[] bytes = new byte[SIZE];
                    final int readSize = stream.read(bytes);
                    if (readSize < SIZE) {
                        throw new CorruptVolumeException("Volume file " + file + " too short: " + readSize);
                    }
                    /*
                     * Check out the fixed Volume file and learn the buffer
                     * size.
                     */
                    if (!verifySignature(bytes)) {
                        throw new CorruptVolumeException("Invalid signature");
                    }

                    final int version = getVersion(bytes);
                    if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) {
                        throw new CorruptVolumeException("Version " + version
                                + " is not supported by Persistit version " + Persistit.version());
                    }
                    final int pageSize = getPageSize(bytes);
                    final long nextAvailablePage = getNextAvailablePage(bytes);
                    final long id = getId(bytes);
                    final long totalPages = file.length() / pageSize;
                    if (totalPages < nextAvailablePage) {
                        throw new CorruptVolumeException(String.format("Volume has been truncated: "
                                + "minimum required/actual lengths=%,d/%,d bytes", nextAvailablePage * pageSize,
                                file.length()));
                    }
                    final long globalTimestamp = getGlobalTimestamp(bytes);
                    if (globalTimestamp > systemTimestamp) {
                        throw new CorruptVolumeException("Volume " + file + " has a global timestamp greater than "
                                + "system timestamp: " + globalTimestamp + " > " + systemTimestamp);
                    }
                    specification.setVersion(version);
                    specification.setPageSize(pageSize);
                    specification.setId(id);
                } else {
                    throw new CorruptVolumeException("Volume file " + file + " is a directory");
                }
                return true;
            } else {
                return false;
            }
        } catch (final IOException ioe) {
            throw new PersistitIOException(ioe);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (final IOException e) {
                }
            }
        }
    }

    /**
     * @return The version number of volumes created by this version of
     *         Persistit.
     */
    public static int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Cmd("volumeinfo")
    static VolumeInfoTask createTask(@Arg("file|string:|Volume file") final String file) throws Exception {
        return new VolumeInfoTask(file);
    }

    private static class VolumeInfoTask extends Task {
        String _volumeFileName;

        private VolumeInfoTask(final String fileName) {
            _volumeFileName = fileName;
        }

        @Override
        protected void runTask() throws Exception {
            final File file = new File(_volumeFileName);
            final FileInputStream stream = new FileInputStream(file);
            final byte[] bytes = new byte[SIZE];
            final int readSize = stream.read(bytes);
            stream.close();
            if (readSize < SIZE) {
                throw new CorruptVolumeException(String.format(
                        "File %s of size %,d is too short to be a Volume file\n", file, readSize));
            }
            /*
             * Check out the fixed Volume file and learn the buffer size.
             */
            if (!verifySignature(bytes)) {
                throw new CorruptVolumeException(String.format("File %s is not a Volume file: Invalid signature", file));
            }
            postMessage(String.format("%30s: %s", "File", file), Task.LOG_NORMAL);
            outn("ID", getId(bytes));
            outn("Page size", getPageSize(bytes));
            outd("Creation time", getCreateTime(bytes));
            outd("Last open time", getOpenTime(bytes));
            outd("Last extension time", getLastExtensionTime(bytes));
            outd("Last read time", getLastReadTime(bytes));
            outd("Last write time", getLastWriteTime(bytes));
            outn("Global timestamp", getGlobalTimestamp(bytes));
            outn("Timestamp", getTimestamp(bytes));
            outn("Version", getVersion(bytes));

            outn("Directory root", getDirectoryRoot(bytes));
            outn("Garbage root", getGarbageRoot(bytes));
            outn("Initial pages", getInitialPages(bytes));
            outn("Next available page", getNextAvailablePage(bytes));
            outn("Extended page count", getExtendedPageCount(bytes));
            outn("Pages per extension", getExtensionPages(bytes));
            outn("Maximum pages", getMaximumPages(bytes));

            outn("Fetch counter", getfetchCounter(bytes));
            outn("Get counter", getGetCounter(bytes));
            outn("Read counter", getReadCounter(bytes));
            outn("Remove counter", getRemoveCounter(bytes));
            outn("Store counter", getStoreCounter(bytes));
            outn("Traverse counter", getTraverseCounter(bytes));
            outn("Write counter", getWriteCounter(bytes));

        }

        private void outd(final String legend, final long value) {
            postMessage(String.format("%30s: %s", legend, SDF.format(new Date(value))), Task.LOG_NORMAL);
        }

        private void outn(final String legend, final long value) {
            postMessage(String.format("%30s: %,12d", legend, value), Task.LOG_NORMAL);
        }

        @Override
        public String getStatus() {
            return "";
        }

    }

    public static void main(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("VolumeHeader", args, ARGS_TEMPLATE).strict();
        if (ap.isUsageOnly()) {
            return;
        }
        final Task task = new VolumeInfoTask(ap.getStringValue("path"));
        task.setMessageWriter(new PrintWriter(System.out));
        task.runTask();
    }

}
