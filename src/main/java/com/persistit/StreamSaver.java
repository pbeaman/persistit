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

package com.persistit;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

/**
 * <p>
 * Saves Persistit records to a DataOutputStream in the format expected by a
 * {@link StreamLoader} instance.
 * </p>
 * 
 * @version 1.0
 */
public class StreamSaver extends Task {

    /**
     * Record type marker for FILL records
     */
    public final static int RECORD_TYPE_FILL = ('A' << 8) + 'Z';
    /**
     * Record type marker for DATA records
     */
    public final static int RECORD_TYPE_DATA = ('D' << 8) + 'R';
    /**
     * Record type marker for KEY_FILTER records
     */
    public final static int RECORD_TYPE_KEY_FILTER = ('K' << 8) + 'F';
    /**
     * Record type marker for VOLUME_ID records
     */
    public final static int RECORD_TYPE_VOLUME_ID = ('V' << 8) + 'I';
    /**
     * Record type marker for TREE_ID records
     */
    public final static int RECORD_TYPE_TREE_ID = ('T' << 8) + 'I';
    /**
     * Record type marker for HOSTNAME records
     */
    public final static int RECORD_TYPE_HOSTNAME = ('H' << 8) + 'N';
    /**
     * Record type marker for USER records
     */
    public final static int RECORD_TYPE_USER = ('H' << 8) + 'U';
    /**
     * Record type marker for COMMENT records
     */
    public final static int RECORD_TYPE_COMMENT = ('C' << 8) + 'O';
    /**
     * Record type marker for FILL records
     */
    public final static int RECORD_TYPE_COUNT = ('R' << 8) + 'C';
    /**
     * Record type marker for START records
     */
    public final static int RECORD_TYPE_START = ('X' << 8) + 'S';
    /**
     * Record type marker for END records
     */
    public final static int RECORD_TYPE_END = ('X' << 8) + 'E';
    /**
     * Record type marker for TIMESTAMP records
     */
    public final static int RECORD_TYPE_TIMESTAMP = ('T' << 8) + 'S';
    /**
     * Record type marker for EXCEPTION records
     */
    public final static int RECORD_TYPE_EXCEPTION = ('E' << 8) + 'X';
    /**
     * Record type marker for COMPLETION records
     */
    public final static int RECORD_TYPE_COMPLETION = ('Z' << 8) + 'Z';
    /**
     * Default count of records to written with elided keys.
     */
    public final static int DEFAULT_CYCLE_COUNT = 1024;
    /**
     * Size of the buffer for BufferedOutputStreams created by the File
     * constructors.
     */
    public final static int DEFAULT_BUFFER_SIZE = 65536;

    protected String _filePath;
    protected DataOutputStream _dos;

    protected Key _lastKey;
    protected Volume _lastVolume;
    protected Tree _lastTree;
    protected long _dataRecordCount = 0;
    protected long _otherRecordCount = 0;
    protected int _cycleCount = DEFAULT_CYCLE_COUNT;
    protected boolean _stop;
    protected Exception _lastException;
    protected int _recordCount;
    protected TreeSelector _treeSelector;

    /**
     * Package-private constructor used by {@link ManagementImpl} to instantiate
     * a {@link Task}. The {@link #setupTask} method must be called to specify
     * trees and the output file name.
     * 
     */
    StreamSaver() {
        _lastKey = new Key((Persistit) null);
    }

    /**
     * Construct a StreamSaver from the provided DataOutputStream. The
     * DataOutputStream should be based on a BufferedOutputStream for better
     * performance.
     * 
     * @param dos
     *            The DataOutputStream
     */
    public StreamSaver(final Persistit persistit, final DataOutputStream dos) {
        super(persistit);
        _lastKey = new Key(persistit);
        _dos = dos;
    }

    /**
     * Construct a StreamSaver from the provided File using a default buffer
     * size of 64K bytes.
     * 
     * @param file
     *            The File to which data will be saved
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final File file) throws FileNotFoundException {
        this(persistit, new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), DEFAULT_BUFFER_SIZE)));
    }

    @Cmd("save")
    static StreamSaver createTask(@Arg("file|string:|Save to file") final String file,
            @Arg("trees|string:*|Tree selector - specify Volumes/Trees/Keys to save") final String treeSelectorString,
            @Arg("_flag|v|verbose") final boolean verbose,
            @Arg("_flag|r|Use regular expressions in tree selector") final boolean regex) throws Exception {
        final StreamSaver task = new StreamSaver();
        task._filePath = file;
        task._treeSelector = TreeSelector.parseSelector(treeSelectorString, regex, '\\');
        task.setMessageLogVerbosity(verbose ? LOG_VERBOSE : LOG_NORMAL);
        return task;
    }

    /**
     * Construct a StreamSaver from the provided path name using a default
     * buffer size of 64K bytes.
     * 
     * @param pathName
     *            The path name of the file to which data will be saved
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final String pathName) throws FileNotFoundException {
        this(persistit, new DataOutputStream(new BufferedOutputStream(new FileOutputStream(pathName),
                DEFAULT_BUFFER_SIZE)));
    }

    /**
     * Construct a StreamSaver from the provided File using a specified buffer
     * size.
     * 
     * @param file
     *            The File to which data will be saved
     * @param bufferSize
     *            The buffer size
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final File file, final int bufferSize) throws FileNotFoundException {
        this(persistit, new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), bufferSize)));
    }

    /**
     * Construct a StreamSaver from the provided path name using a specified
     * buffer size.
     * 
     * @param pathName
     *            The path name of the file to which data will be saved
     * @param bufferSize
     *            The buffer size
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final String pathName, final int bufferSize)
            throws FileNotFoundException {
        this(persistit, new DataOutputStream(new BufferedOutputStream(new FileOutputStream(pathName), bufferSize)));
    }

    /**
     * Returns the cycle count, which is the maximum number of records that will
     * be written with elided keys. When this number of records is reached,
     * <code>StreamSaver</code> writes the next record with a full, unelided key
     * value. This permits easier visual inspection of the save file.
     * 
     * @return The cycle count
     */
    public int getCycleCount() {
        return _cycleCount;
    }

    /**
     * Sets the cycle count. See {@link #getCycleCount} for a description of
     * this property.
     * 
     * @param count
     *            The cycle count
     */
    public void setCycleCount(final int count) {
        _cycleCount = count;
    }

    /**
     * Closes this StreamSaver and the underlying DataOutputStream. If the save
     * operation ran to completion without error, this method writes a
     * COMPLETION record to the save file before closing it. This record
     * indicates that the save file represents all the records requested by the
     * save operation.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        writeTimestamp();
        if (!_stop && _lastException == null)
            _dos.writeChar(RECORD_TYPE_COMPLETION);
        _lastTree = null;
        _lastVolume = null;
        _lastKey.clear();
        _dos.close();
    }

    /**
     * Writes the key/value pair represented by the current state of an
     * <code>Exchange</code> into a DATA record. If the supplied
     * <code>Exchange</code> is based on different volume or tree than the
     * previously written record, this method also writes VOLUME_ID and/or
     * TREE_ID records before the DATA record. This allows the
     * <code>StreamLoader</code> to apply the data record to the correct volume
     * and tree.
     * 
     * @param exchange
     *            The <code>Exchange</code>
     * @throws IOException
     */
    protected void writeData(final Exchange exchange) throws IOException {
        if (_lastVolume != exchange.getVolume()) {
            writeVolumeInfo(exchange);
        }

        if (_lastTree != exchange.getTree()) {
            writeTreeInfo(exchange);
        }

        writeData(exchange.getKey(), exchange.getValue());
        _recordCount++;
        if ((_recordCount % 100) == 0)
            poll();
    }

    /**
     * Writes a key/value pair into a DATA record.
     * 
     * @param key
     *            The <code>Key</code>
     * @param value
     *            The <code>Value</code>
     * @throws IOException
     */
    protected void writeData(final Key key, final Value value) throws IOException {
        final int elisionCount = key.firstUniqueByteIndex(_lastKey);
        _dos.writeChar(RECORD_TYPE_DATA);
        _dos.writeShort(key.getEncodedSize());
        _dos.writeShort(elisionCount);
        _dos.writeInt(value.getEncodedSize());
        _dos.write(key.getEncodedBytes(), elisionCount, key.getEncodedSize() - elisionCount);
        _dos.write(value.getEncodedBytes(), 0, value.getEncodedSize());

        _dataRecordCount++;
        key.copyTo(_lastKey);

        if (_cycleCount != 0 & (_dataRecordCount % _cycleCount) == 0) {
            writeRecordCount(_dataRecordCount, _otherRecordCount);
            _lastKey.clear();
        }
    }

    /**
     * Writes a RECORD_COUNT record. This record conveys the counts of data
     * records and non-data records written to the stream so far, and can be
     * used to check the integrity of the save file. The RECORD_COUNT record is
     * preceded by three FILL records to allow for easier inspection of the save
     * file.
     * 
     * @throws IOException
     */
    protected void writeRecordCount(final long dataRecordCount, final long otherRecordCount) throws IOException {
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_COUNT);
        _dos.writeLong(dataRecordCount);
        _dos.writeLong(otherRecordCount);
        _otherRecordCount++;
    }

    /**
     * Writes a VOLUME_ID record for the <code>Volume</code> currently
     * associated with the supplied <code>Exchange</code>. The information is
     * sufficient to recreate a new, empty <code>Volume</code> having the same
     * path name, original size and growth parameters as the <code>Volume</code>
     * being saved.
     * 
     * @param exchange
     *            The <code>Exchange</code>
     * @throws IOException
     */
    protected void writeVolumeInfo(final Exchange exchange) throws IOException {
        writeVolumeInfo(exchange.getVolume());
        _lastVolume = exchange.getVolume();
    }

    /**
     * Writes a TREE_ID record for the <code>Tree</code> currently associated
     * with the supplied <code>Exchange</code>. The information is sufficient to
     * recreate a new, empty <code>Tree</code> having the same name as the
     * <code>Tree</code> being saved.
     * 
     * @param exchange
     * @throws IOException
     */
    protected void writeTreeInfo(final Exchange exchange) throws IOException {
        writeTreeInfo(exchange.getTree());
        _lastTree = exchange.getTree();
    }

    /**
     * Writes a VOLUME_ID record for the supplied <code>Volume</code>. The saved
     * information is sufficient to recreate a new, empty <code>Volume</code>
     * having the same path name, original size and growth parameters as the
     * <code>Volume</code> being saved.
     * 
     * @param volume
     *            The <code>Volume</code>
     * @throws IOException
     */
    protected void writeVolumeInfo(final Volume volume) throws IOException {
        _dos.writeChar(RECORD_TYPE_VOLUME_ID);
        _dos.writeLong(volume.getId());
        _dos.writeLong(volume.getSpecification().getInitialPages());
        _dos.writeLong(volume.getSpecification().getExtensionPages());
        _dos.writeLong(volume.getSpecification().getMaximumPages());
        _dos.writeInt(volume.getStructure().getPageSize());
        _dos.writeUTF(volume.getPath());
        _dos.writeUTF(volume.getName());
        _lastVolume = volume;
        _lastTree = null;
        _otherRecordCount++;
    }

    /**
     * Writes a TREE_ID record for the supplied <code>Tree</code>. The saved
     * information is sufficient to recreate a new, empty <code>Tree</code>
     * having the same name as the <code>Tree</code> being saved.
     * 
     * @param tree
     *            The <code>Tree</code>
     * @throws IOException
     */
    protected void writeTreeInfo(final Tree tree) throws IOException {
        _dos.writeChar(RECORD_TYPE_TREE_ID);
        _dos.writeUTF(tree.getName());
        _lastTree = tree;
        _otherRecordCount++;
    }

    /**
     * Writes a TIMESTAMP record containing the current system time.
     * 
     * @throws IOException
     */
    protected void writeTimestamp() throws IOException {
        _dos.writeChar(RECORD_TYPE_TIMESTAMP);
        _dos.writeLong(System.currentTimeMillis());
    }

    /**
     * Writes a COMMENT record containing an arbitrary string
     * 
     * @param comment
     *            The comment string
     * @throws IOException
     */
    protected void writeComment(final String comment) throws IOException {
        _dos.writeChar(RECORD_TYPE_COMMENT);
        _dos.writeUTF(Util.NEW_LINE + "//" + comment + "//");
    }

    /**
     * Writes an EXCEPTION record, indicating an Exception that occurred during
     * the save process.
     * 
     * @param exception
     *            The </code>Exception</code>
     * @throws IOException
     */
    protected void writeException(final Exception exception) throws IOException {
        _dos.writeChar(RECORD_TYPE_EXCEPTION);
        _dos.writeUTF(exception.toString());
    }

    /**
     * Save all key/value pairs in the <code>Tree</code> associated with the
     * supplied <code>Exchange</code>, subject to selection by the supplied
     * <code>KeyFilter</code>. If the <code>filter</code> is <code>null</code>
     * then save all records.
     * 
     * @param exchange
     *            The <code>Exchange</code>
     * @param filter
     *            The <code>KeyFilter</code>
     * @throws PersistitException
     * @throws IOException
     */
    public void save(final Exchange exchange, final KeyFilter filter) throws PersistitException, IOException {
        postMessage("Saving Tree " + exchange.getTree().getName() + " in volume " + exchange.getVolume().getPath()
                + (filter == null ? "" : " using KeyFilter: " + filter.toString()), LOG_VERBOSE);

        writeTimestamp();
        _dos.writeChar(RECORD_TYPE_START);
        if (filter != null) {
            _dos.writeChar(RECORD_TYPE_KEY_FILTER);
            _dos.writeUTF(filter.toString());
        }
        final Key key = exchange.getKey();
        key.clear().append(Key.BEFORE);
        while (exchange.traverse(Key.GT, filter, Integer.MAX_VALUE) & !_stop) {
            writeData(exchange);
        }
        writeRecordCount(_dataRecordCount, _otherRecordCount);
        _dos.writeChar(RECORD_TYPE_END);
        writeTimestamp();
        poll();
    }

    /**
     * Saves one or more trees in a named volume. The volume is specified by
     * <code>volumeName</code>, which may either be the full path name of an
     * open volume, or a substring that uniquely matches one open volume.
     * 
     * @param volumeName
     *            The volume name, or a substring that matches only one volume.
     * @param selectedTreeNames
     *            An array names of the trees to be saved.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(final String volumeName, final String[] selectedTreeNames) throws PersistitException,
            IOException {
        final Volume volume = _persistit.getVolume(volumeName);
        if (volume != null)
            saveTrees(volume, selectedTreeNames);
    }

    /**
     * Saves one or more trees in the specified <code>Volume</code>.
     * 
     * @param volume
     *            The <code>Volume</code>
     * @param selectedTreeNames
     *            An array names of the trees to be saved.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(final Volume volume, final String[] selectedTreeNames) throws PersistitException, IOException {
        final String[] treeNames = volume.getTreeNames();
        writeComment("Volume " + volume.getPath());
        for (int index = 0; index < treeNames.length & !_stop; index++) {
            boolean selected = true;
            if (selectedTreeNames != null) {
                for (int index2 = 0; selected && index2 < selectedTreeNames.length; index2++) {
                    if (!selectedTreeNames[index2].equals(treeNames[index])) {
                        selected = false;
                    }
                }
            }
            if (!selected) {
                writeComment("Tree " + treeNames[index] + " not selected");
            } else {
                writeComment("Tree " + treeNames[index]);
                try {
                    final Exchange exchange = _persistit.getExchange(volume, treeNames[index], false);
                    save(exchange, null);
                } catch (final PersistitException exception) {
                    _lastException = exception;
                    writeException(exception);
                }
            }
        }
    }

    /**
     * Saves one or more trees.
     * 
     * @param treeSelector
     *            The <code>TreeSelector</code>s to select volumes, trees, and
     *            KeyFilters within trees.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(final TreeSelector treeSelector) throws PersistitException, IOException {
        final List<Tree> trees = _persistit.getSelectedTrees(treeSelector);
        for (final Tree tree : trees) {
            if (tree.getVolume().getDirectoryTree() == tree) {
                for (final String treeName : tree.getVolume().getTreeNames()) {
                    final Tree t = tree.getVolume().getTree(treeName, false);
                    try {
                        writeComment("Tree " + treeName + " in " + tree.getVolume().getPath());
                        final Exchange exchange = new Exchange(t);
                        save(exchange, null);
                    } catch (final PersistitException exception) {
                        _lastException = exception;
                        writeException(exception);
                    }
                }
            } else {
                try {
                    writeComment("Tree " + tree.getName() + " in " + tree.getVolume().getPath());
                    final Exchange exchange = new Exchange(tree);
                    save(exchange, treeSelector.keyFilter(tree.getVolume().getName(), tree.getName()));
                } catch (final PersistitException exception) {
                    _lastException = exception;
                    writeException(exception);
                }
            }
        }
    }

    /**
     * Save all trees in all open volumes.
     * 
     * @throws PersistitException
     * @throws IOException
     */
    public void saveAll() throws PersistitException, IOException {

        for (final Volume volume : _persistit.getVolumes()) {
            if (_stop) {
                break;
            }
            saveTrees(volume, null);
        }
    }

    @Override
    protected void runTask() throws PersistitException, IOException {
        _dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_filePath), DEFAULT_BUFFER_SIZE));
        saveTrees(_treeSelector);
        close();
    }

    @Override
    public String getStatus() {
        if (_lastTree == null) {
            return null;
        } else {
            return "Saving " + _lastTree.getName() + " in " + _lastTree.getVolume().getPath() + " (" + _recordCount
                    + ")";
        }
    }
}
