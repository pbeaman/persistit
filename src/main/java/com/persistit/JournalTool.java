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

import static com.persistit.JournalRecord.OVERHEAD;
import static com.persistit.JournalRecord.SUB_RECORD_OVERHEAD;
import static com.persistit.JournalRecord.getLength;
import static com.persistit.JournalRecord.getTimestamp;
import static com.persistit.JournalRecord.getType;
import static com.persistit.JournalRecord.isValidType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.persistit.JournalRecord.CP;
import com.persistit.JournalRecord.D0;
import com.persistit.JournalRecord.D1;
import com.persistit.JournalRecord.DR;
import com.persistit.JournalRecord.DT;
import com.persistit.JournalRecord.IT;
import com.persistit.JournalRecord.IV;
import com.persistit.JournalRecord.JE;
import com.persistit.JournalRecord.JH;
import com.persistit.JournalRecord.PA;
import com.persistit.JournalRecord.PM;
import com.persistit.JournalRecord.SR;
import com.persistit.JournalRecord.TM;
import com.persistit.JournalRecord.TX;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.mxbeans.JournalManagerMXBean;
import com.persistit.util.ArgParser;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class JournalTool {

    public final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

    public final static long DEFAULT_BLOCK_SIZE = 1000000000L;

    private final static int EOF = -1;

    private final static int EOJ = -2;

    final static String[] ARGS_TEMPLATE = { "path|string:|Journal file name",
            "start|long:0:0:10000000000000|Start journal address",
            "end|long:1000000000000000000:0:1000000000000000000|End journal address",
            "types|String:*|Selected record types, for example, \"PA,PM,CP\"",
            "pages|String:*|Selected pages, for example, \"0,1,200-299,33333-\"",
            "timestamps|String:*|Selected timestamps, for example, \"132466-132499\"",
            "maxkey|int:42:4:10000|Maximum displayed key length",
            "maxvalue|int:42:4:100000|Maximum displayed value length",
            "timestamps|String:*|Selected timestamps, for example, \"132466-132499\"",
            "_flag|v|Verbose dump - includes PageMap and TransactionMap details" };

    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    private final Persistit _persistit;

    private Action _action;

    private String _journalFilePath;

    private long _startAddr;

    private long _endAddr;

    private PrintWriter _writer = new PrintWriter(new OutputStreamWriter(System.out));

    private final Map<Long, FileChannel> _journalFileChannels = new HashMap<Long, FileChannel>();

    private ByteBuffer _readBuffer;

    private final int _readBufferSize = DEFAULT_BUFFER_SIZE;

    private long _readBufferAddress;

    private long _currentAddress;

    private long _blockSize = DEFAULT_BLOCK_SIZE;

    private BitSet _selectedTypes = new BitSet(65536);

    private RangePredicate _selectedPages;

    private RangePredicate _selectedTimestamps;

    private int _keyDisplayLength;

    private int _valueDisplayLength;

    private boolean _verbose;

    public long getStartAddr() {
        return _startAddr;
    }

    public void setStartAddr(final long startAddr) {
        _startAddr = startAddr;
    }

    public long getEndAddr() {
        return _endAddr;
    }

    public void setEndAddr(final long endAddr) {
        _endAddr = endAddr;
    }

    public Action getAction() {
        return _action;
    }

    public void setAction(final Action action) {
        _action = action;
    }

    public PrintWriter getWriter() {
        return _writer;
    }

    public void setWriter(final PrintWriter writer) {
        _writer = writer;
    }

    public BitSet getSelectedTypes() {
        return _selectedTypes;
    }

    public void setSelectedTypes(final BitSet selectedTypes) {
        _selectedTypes = selectedTypes;
    }

    public RangePredicate getSelectedPages() {
        return _selectedPages;
    }

    public void setSelectedPages(final RangePredicate selectedPages) {
        _selectedPages = selectedPages;
    }

    public RangePredicate getSelectedTimestamps() {
        return _selectedTimestamps;
    }

    public void setSelectedTimestamps(final RangePredicate selectedTimestamps) {
        _selectedTimestamps = selectedTimestamps;
    }

    public int getKeyDisplayLength() {
        return _keyDisplayLength;
    }

    public void setKeyDisplayLength(final int keyDisplayLength) {
        _keyDisplayLength = keyDisplayLength;
    }

    public int getValueDisplayLength() {
        return _valueDisplayLength;
    }

    public void setValueDisplayLength(final int valueDisplayLength) {
        _valueDisplayLength = valueDisplayLength;
    }

    public boolean isVerbose() {
        return _verbose;
    }

    public void setVerbose(final boolean verbose) {
        _verbose = verbose;
    }

    public interface Action {
        public void je(final long address, final long timestamp, final int recordSize) throws Exception;

        public void jh(final long address, final long timestamp, final int recordSize) throws Exception;

        public void iv(final long address, final long timestamp, final int recordSize) throws Exception;

        public void it(final long address, final long timestamp, final int recordSize) throws Exception;

        public void cp(final long address, final long timestamp, final int recordSize) throws Exception;

        public void tx(final long address, final long timestamp, final int recordSize) throws Exception;

        public void sr(final long address, final long timestamp, final int recordSize) throws Exception;

        public void dr(final long address, final long timestamp, final int recordSize) throws Exception;

        public void dt(final long address, final long timestamp, final int recordSize) throws Exception;

        public void pa(final long address, final long timestamp, final int recordSize) throws Exception;

        public void pm(final long address, final long timestamp, final int recordSize) throws Exception;

        public void tm(final long address, final long timestamp, final int recordSize) throws Exception;

        public void d0(final long address, final long timestamp, final int recordSize) throws Exception;

        public void d1(final long address, final long timestamp, final int recordSize) throws Exception;

        public void eof(final long address) throws Exception;
    }

    protected static class RangePredicate {
        private final long[] _left;
        private final long[] _right;

        RangePredicate(final String ps) {
            if ("*".equals(ps)) {
                _left = new long[0];
                _right = new long[0];
            } else {
                final String[] terms = ps.split(",");
                _left = new long[terms.length];
                _right = new long[terms.length];
                for (int index = 0; index < terms.length; index++) {
                    final String[] range = terms[index].split("-");
                    boolean okay = true;
                    try {
                        switch (range.length) {
                        case 1:
                            final long v = Long.parseLong(range[0]);
                            _left[index] = v;
                            _right[index] = v;
                            break;

                        case 2:
                            if (range[0].isEmpty()) {
                                _left[index] = Long.MIN_VALUE;
                            } else {
                                _left[index] = Long.parseLong(range[0]);
                            }
                            if (range[1].isEmpty()) {
                                _right[index] = Long.MAX_VALUE;
                            } else {
                                _right[index] = Long.parseLong(range[1]);
                            }
                            break;
                        default:
                            okay = false;
                        }
                    } catch (final NumberFormatException e) {
                        okay = false;
                    }
                    if (!okay) {
                        throw new IllegalArgumentException("Invalid term " + terms[index] + " in range specification "
                                + ps);
                    }
                }
            }
        }

        private boolean isSelected(final long v) {
            if (_left.length == 0) {
                return true;
            }
            for (int index = 0; index < _left.length; index++) {
                if (_left[index] <= v && _right[index] >= v) {
                    return true;
                }
            }
            return false;
        }
    }

    public JournalTool(final Persistit persistit) {
        _persistit = persistit;
        _action = new SimpleDumpAction();
        _selectedTypes.set(0, 65535, true);
    }

    public void init(final String[] args) {
        final ArgParser ap = new ArgParser("com.persistit.JournalTool", args, ARGS_TEMPLATE).strict();
        if (ap.isUsageOnly()) {
            return;
        }
        init(ap);
    }

    void init(final ArgParser ap) {
        init(ap.getStringValue("path"), ap.getLongValue("start"), ap.getLongValue("end"), ap.getStringValue("types"),
                ap.getStringValue("pages"), ap.getStringValue("timestamps"), ap.getIntValue("maxkey"),
                ap.getIntValue("maxvalue"), ap.isFlag('v'));
    }

    void init(final String path, final long start, final long end, final String types, final String pages,
            final String timestamps, final int maxkey, final int maxvalue, final boolean v) {
        _startAddr = start;
        _endAddr = end;
        String pathName = path;
        if (isNullOrEmpty(pathName) && _persistit != null) {
            pathName = _persistit.getJournalManager().getJournalFilePath();
        }
        if (isNullOrEmpty(pathName)) {
            throw new IllegalArgumentException(
                    "The 'path' parameter must specify a valid journal path, for example, \n"
                            + " /xxx/yyy/jjj where journal file names " + "are like jjj.000000001234");
        }
        final long generation = JournalManager.fileToGeneration(new File(pathName));
        if (generation != -1) {
            pathName = pathName.substring(0, pathName.lastIndexOf('.'));
            if (start == 0) {
                _startAddr = generation * JournalManagerMXBean.DEFAULT_BLOCK_SIZE;
            }
            if (end == 1000000000000000000l) {
                _endAddr = (generation + 1) * JournalManagerMXBean.DEFAULT_BLOCK_SIZE;
            }
        }
        _journalFilePath = pathName;
        _readBuffer = ByteBuffer.allocate(_readBufferSize);
        parseTypes(types);
        _selectedPages = new RangePredicate(pages);
        _selectedTimestamps = new RangePredicate(timestamps);
        _keyDisplayLength = maxkey;
        _valueDisplayLength = maxvalue;
        _verbose = v;

    }

    private boolean isNullOrEmpty(final String string) {
        return string == null || string.isEmpty();
    }

    private void parseTypes(final String types) {
        boolean okay = true;
        if (!("*".equals(types))) {
            _selectedTypes.set(0, _selectedTypes.size(), false);
            for (final String typeString : types.split(",")) {
                if (typeString.length() == 2) {
                    final int t = (typeString.charAt(0) << 8) | typeString.charAt(1);
                    if (JournalRecord.isValidType(t)) {
                        _selectedTypes.set(t, true);
                    } else {
                        okay = false;
                    }
                } else {
                    okay = false;
                }
            }
        }
        if (!okay) {
            throw new IllegalArgumentException("The 'types' parameter must specify either \"*\" or a "
                    + "comma-separated list of valid record type names");
        }
    }

    public void scan() throws Exception {
        try {
            _currentAddress = _startAddr;
            _readBufferAddress = Long.MIN_VALUE;
            while (_currentAddress < _endAddr) {
                final int type = scanOneRecord();
                switch (type) {
                case JE.TYPE:
                case EOF:
                    _currentAddress = addressUp(_currentAddress);
                    break;
                case EOJ:
                    return;
                }
            }
        } finally {
            _writer.flush();
        }
    }

    /**
     * Attempts to read and apply the record at _currentAddress. If it finds
     * valid record contained in the current journal file, it advances the
     * _currentAddress to the start of the next record and returns the type of
     * the record. Otherwise this method does nothing and returns -1;
     * 
     * @return The record type: one of the type values specified in
     *         {@link com.persistit.JournalRecord}), 0 if the journal file has
     *         fewer than 16 bytes remaining or -t where t is an invalid type.
     * @throws CorruptJournalException
     * @throws PersistitException
     * @throws JournalNotClosedException
     */
    private int scanOneRecord() throws Exception {

        final long from = _currentAddress;
        try {
            read(_currentAddress, OVERHEAD);
        } catch (final CorruptJournalException e) {
            // This exception means abnormal end-of-file.
            _action.eof(from);
            return EOF;
        } catch (final PersistitIOException e) {
            if (e.getCause() instanceof FileNotFoundException) {
                return EOJ;
            } else {
                throw e;
            }
        }
        final int recordSize = getLength(_readBuffer);
        final int type = getType(_readBuffer);
        final long timestamp = getTimestamp(_readBuffer);
        _currentAddress = processOneRecord(from, timestamp, recordSize, type);
        return type;
    }

    long processOneRecord(final long from, final long timestamp, final int recordSize, final int type) throws Exception {
        if (recordSize >= _blockSize || recordSize < SUB_RECORD_OVERHEAD) {
            throw new CorruptJournalException("Bad JournalRecord length " + recordSize + " at position "
                    + addressToString(from, timestamp));
        }
        long address = from;
        switch (type) {

        case JE.TYPE:
            if (_selectedTypes.get(type)) {
                _action.je(address, timestamp, recordSize);
            }
            break;

        case JH.TYPE:
            read(_currentAddress, recordSize);
            final long blockSize = JH.getBlockSize(_readBuffer);
            if (blockSize != _blockSize) {
                address = _currentAddress = (_currentAddress / _blockSize) * blockSize;
                _readBufferAddress = _currentAddress;
                _blockSize = blockSize;
            }
            if (_selectedTypes.get(type)) {
                _action.jh(address, timestamp, recordSize);
            }
            break;

        case SR.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.sr(address, timestamp, recordSize);
            }
            break;

        case DR.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.dr(address, timestamp, recordSize);
            }
            break;

        case DT.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.dt(address, timestamp, recordSize);
            }
            break;

        case IV.TYPE:
            if (_selectedTypes.get(type)) {
                _action.iv(address, timestamp, recordSize);
            }
            break;

        case IT.TYPE:
            if (_selectedTypes.get(type)) {
                _action.it(address, timestamp, recordSize);
            }
            break;

        case PA.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.pa(address, timestamp, recordSize);
            }
            break;

        case PM.TYPE:
            if (_selectedTypes.get(type)) {
                _action.pm(address, timestamp, recordSize);
            }
            break;

        case TM.TYPE:
            if (_selectedTypes.get(type)) {
                _action.tm(address, timestamp, recordSize);
            }
            break;

        case TX.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.tx(address, timestamp, recordSize);
            }
            break;

        case CP.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.cp(address, timestamp, recordSize);
            }
            break;

        case D0.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.d0(address, timestamp, recordSize);
            }
            break;

        case D1.TYPE:
            if (_selectedTypes.get(type) && _selectedTimestamps.isSelected(timestamp)) {
                _action.d1(address, timestamp, recordSize);
            }
            break;

        default:
            if (!isValidType(type)) {
                _currentAddress -= OVERHEAD;
                throw new CorruptJournalException("Invalid record type " + type + " at " + addressToString(address));
            }
        }
        return address + recordSize;

    }

    private long addressUp(final long address) {
        return ((address / _blockSize) + 1) * _blockSize;
    }

    private synchronized FileChannel getFileChannel(final long address) throws PersistitIOException {
        final long generation = address / _blockSize;
        FileChannel channel = _journalFileChannels.get(generation);
        if (channel == null) {
            try {
                final RandomAccessFile raf = new RandomAccessFile(addressToFile(address), "r");
                channel = raf.getChannel();
                _journalFileChannels.put(generation, channel);
            } catch (final IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }
        return channel;
    }

    File addressToFile(final long address) {
        return JournalManager.generationToFile(_journalFilePath, address / _blockSize);
    }

    private void read(final long address, final int size) throws PersistitIOException {
        if (_readBufferAddress >= 0 && address >= _readBufferAddress
                && size + address - _readBufferAddress <= _readBuffer.limit()) {
            _readBuffer.position((int) (address - _readBufferAddress));
        } else {
            try {
                final FileChannel fc = getFileChannel(address);
                _readBuffer.clear();

                int maxSize = _readBuffer.capacity();
                final long remainingInBlock = addressUp(address) - address;
                if (remainingInBlock < maxSize) {
                    maxSize = (int) remainingInBlock;
                }

                _readBuffer.limit(maxSize);
                // Try to read up to maxSize bytes into _readBuffer
                int offset = 0;
                while (_readBuffer.remaining() > 0) {
                    final int readSize = fc.read(_readBuffer, offset + address % _blockSize);
                    if (readSize < 0) {
                        break;
                    }
                    offset += readSize;
                }
                _readBufferAddress = address;
                _readBuffer.flip();
                if (_readBuffer.remaining() < size) {
                    throw new CorruptJournalException("End of file at " + addressToString(address));
                }
            } catch (final IOException e) {
                throw new PersistitIOException("Reading from " + addressToString(address), e);
            }
        }
    }

    private String addressToString(final long address) {
        return String.format("JournalAddress %,d", address);
    }

    private String addressToString(final long address, final long timestamp) {
        return String.format("JournalAddress %,d{%,d}", address, timestamp);
    }

    // -----------------------

    public static void main(final String[] args) throws Exception {
        final Persistit persistit = new Persistit();
        JournalTool jt = null;
        try {
            jt = new JournalTool(persistit);
            jt.init(args);
        } catch (final IllegalArgumentException e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
        try {
            jt.scan();
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            persistit.close();
        }
    }

    protected class SimpleDumpAction implements Action {

        final static String ELIPSES = "...";

        final StringBuilder sb = new StringBuilder();

        final Key key1 = new Key(_persistit);

        final Key key2 = new Key(_persistit);

        final Value value = new Value(_persistit);

        protected void start(final long address, final long timestamp, final String type, final int recordSize) {
            sb.setLength(0);
            sb.append(String.format("%,18d%,16d %2s (%8d) ", address, timestamp, type, recordSize));
        }

        protected void appendf(final String format, final Object... args) {
            sb.append(String.format(format, args));
        }

        protected void keyf(final Key key) {
            String s = null;
            try {
                s = key.toString();
            } catch (final Exception e) {
                s = e.getLocalizedMessage();
            }
            padf(s, _keyDisplayLength);
        }

        protected void valuef(final Value value) {
            String s = null;
            try {
                if (value.getEncodedSize() >= Buffer.LONGREC_SIZE
                        && (value.getEncodedBytes()[0] & 0xFF) == Buffer.LONGREC_TYPE) {
                    final long page = Buffer.decodeLongRecordDescriptorPointer(value.getEncodedBytes(), 0);
                    final int size = Buffer.decodeLongRecordDescriptorSize(value.getEncodedBytes(), 0);
                    s = String.format("LONG_REC size %,8d page %12d", size, page);
                } else {
                    s = value.toString();
                }
            } catch (final Exception e) {
                s = e.getLocalizedMessage();
            }
            padf(s, _valueDisplayLength);
        }

        protected void padf(final String s, final int length) {
            if (s.length() < length) {
                sb.append(s);
                for (int i = length - s.length(); --i >= 0;) {
                    sb.append(' ');
                }
            } else {
                sb.append(s.substring(0, length - ELIPSES.length()));
                sb.append(ELIPSES);
            }
        }

        protected void flush() {
            if (sb.length() > 0) {
                write(sb.toString());
                sb.setLength(0);
            }
        }

        protected void write(final String msg) {
            _writer.println(msg);
        }

        // -----------------------

        @Override
        public void jh(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final long baseAddress = JH.getBaseJournalAddress(_readBuffer);
            final long blockSize = JH.getBlockSize(_readBuffer);
            final String fileCreated = SDF.format(new Date(JH.getFileCreatedTime(_readBuffer)));
            final String journalCreated = SDF.format(new Date(JH.getJournalCreatedTime(_readBuffer)));
            final long version = JH.getVersion(_readBuffer);
            start(address, timestamp, "JH", recordSize);
            appendf(" version %,3d blockSize %,14d baseAddress %,18d journalCreated %24s fileCreated %24s", version,
                    blockSize, baseAddress, journalCreated, fileCreated);
            _blockSize = blockSize;
            flush();
        }

        @Override
        public void je(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            start(address, timestamp, "JE", recordSize);
            final long baseAddress = JE.getBaseAddress(_readBuffer);
            final long currentAddress = JE.getCurrentJournalAddress(_readBuffer);
            final String journalCreated = SDF.format(new Date(JE.getJournalCreatedTime(_readBuffer)));
            appendf(" baseAddress %,18d currentAddress %,18d journalCreated %24s", baseAddress, currentAddress,
                    journalCreated);
            flush();
        }

        @Override
        public void iv(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final int handle = IV.getHandle(_readBuffer);
            final long id = IV.getVolumeId(_readBuffer);
            final String name = IV.getVolumeSpecification(_readBuffer);
            start(address, timestamp, "IV", recordSize);
            appendf(" handle %05d id %,22d name %s", handle, id, name);
            flush();
        }

        @Override
        public void it(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final int handle = IT.getHandle(_readBuffer);
            final int vhandle = IT.getVolumeHandle(_readBuffer);
            final String treeName = IT.getTreeName(_readBuffer);
            start(address, timestamp, "IT", recordSize);
            appendf(" handle %05d volume %05d treeName %s", handle, vhandle, treeName);
            flush();
        }

        @Override
        public void cp(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final long baseAddress = CP.getBaseAddress(_readBuffer);
            final String wallTime = SDF.format(new Date(CP.getSystemTimeMillis(_readBuffer)));
            start(address, timestamp, "CP", recordSize);
            appendf(" baseAddress %,18d at %24s", baseAddress, wallTime);
            flush();
        }

        @Override
        public void tx(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            start(address, timestamp, "TX", recordSize);
            appendf(" committed %,d backchain %,d", TX.getCommitTimestamp(_readBuffer),
                    TX.getBackchainAddress(_readBuffer));
            flush();
            final int start = _readBuffer.position();
            final int end = start + recordSize;
            _readBuffer.position(_readBuffer.position() + TX.OVERHEAD);
            while (_readBuffer.position() < end) {
                final int innerSize = getLength(_readBuffer);
                final int type = getType(_readBuffer);
                final int position = _readBuffer.position();
                processOneRecord(address + position - start, timestamp, innerSize, type);
                _readBuffer.position(position + innerSize);
            }
            flush();
        }

        @Override
        public void dr(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final int thandle = DR.getTreeHandle(_readBuffer);
            final int key1Size = DR.getKey1Size(_readBuffer);
            final int elisionCount = DR.getKey2Elision(_readBuffer);
            final int key2Size = recordSize - key1Size - DR.OVERHEAD;
            System.arraycopy(_readBuffer.array(), _readBuffer.position() + DR.OVERHEAD, key1.getEncodedBytes(), 0,
                    key1Size);
            key1.setEncodedSize(key1Size);
            System.arraycopy(key1.getEncodedBytes(), 0, key2.getEncodedBytes(), 0, elisionCount);
            System.arraycopy(_readBuffer.array(), _readBuffer.position() + DR.OVERHEAD + key1Size,
                    key2.getEncodedBytes(), elisionCount, key2Size);
            key2.setEncodedSize(key2Size + elisionCount);
            start(address, timestamp, "DR", recordSize);
            appendf(" tree %05d key1Size %,5d key2Size %,5d  ", thandle, key1Size, key2Size);
            keyf(key1);
            sb.append("->");
            keyf(key2);
            flush();
        }

        @Override
        public void sr(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final int thandle = SR.getTreeHandle(_readBuffer);
            final int keySize = SR.getKeySize(_readBuffer);
            final int valueSize = recordSize - keySize - SR.OVERHEAD;
            System.arraycopy(_readBuffer.array(), _readBuffer.position() + SR.OVERHEAD, key1.getEncodedBytes(), 0,
                    keySize);
            key1.setEncodedSize(keySize);
            value.ensureFit(valueSize);
            System.arraycopy(_readBuffer.array(), _readBuffer.position() + SR.OVERHEAD + keySize,
                    value.getEncodedBytes(), 0, valueSize);
            value.setEncodedSize(valueSize);
            start(address, timestamp, "SR", recordSize);
            appendf(" tree %05d keySize %,5d valueSize %,5d  ", thandle, keySize, valueSize);
            keyf(key1);
            sb.append(" : ");
            valuef(value);
            flush();
        }

        @Override
        public void dt(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, DT.OVERHEAD);
            final int thandle = DT.getTreeHandle(_readBuffer);
            start(address, timestamp, "DT", recordSize);
            appendf(" tree %05d", thandle);
            flush();
        }

        @Override
        public void pa(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            final long pageAddress = PA.getPageAddress(_readBuffer);
            final int volumeHandle = PA.getVolumeHandle(_readBuffer);
            if (!_selectedPages.isSelected(pageAddress)) {
                return;
            }
            start(address, timestamp, "PA", recordSize);
            final int type = JournalRecord.getByte(_readBuffer, PA.OVERHEAD + Buffer.TYPE_OFFSET);
            final String typeString = Buffer.getPageTypeName(pageAddress, type);
            final long rightSibling = pageAddress == 0 ? 0 : JournalRecord.getLong(_readBuffer, PA.OVERHEAD
                    + Buffer.RIGHT_SIBLING_OFFSET);
            appendf(" page %5d:%,12d type %10s right %,12d", volumeHandle, pageAddress, typeString, rightSibling);
            flush();
        }

        @Override
        public void pm(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, PM.OVERHEAD);
            start(address, timestamp, "PM", recordSize);
            final int count = PM.getEntryCount(_readBuffer);
            appendf(" entries %,10d", count);
            flush();
            if (_verbose) {
                dumpPageMap(count, address, timestamp, recordSize);
                flush();
            }
        }

        @Override
        public void tm(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, TM.OVERHEAD);
            start(address, timestamp, "TM", recordSize);
            final int count = TM.getEntryCount(_readBuffer);
            appendf(" entries %,10d", count);
            flush();
            if (_verbose) {
                dumpTransactionMap(count, address, timestamp, recordSize);
                flush();
            }
        }

        @Override
        public void d0(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            start(address, timestamp, "D0", recordSize);
            final int thandle = D0.getTreeHandle(_readBuffer);
            final int index = D0.getIndex(_readBuffer);
            appendf(" tree %05d index %2d value %,5d", thandle, index, 1);
            flush();
        }

        @Override
        public void d1(final long address, final long timestamp, final int recordSize) throws Exception {
            read(address, recordSize);
            start(address, timestamp, "D1", recordSize);
            final int thandle = D1.getTreeHandle(_readBuffer);
            final int index = D1.getIndex(_readBuffer);
            final long value = D1.getValue(_readBuffer);
            appendf(" tree %05d index %2d value %,5d", thandle, index, value);
            flush();
        }

        @Override
        public void eof(final long address) throws Exception {
            start(address, 0, "~~", 0);
            flush();
        }

        void dumpPageMap(final int count, final long from, final long timestamp, final int recordSize)
                throws PersistitIOException {
            if (count * PM.ENTRY_SIZE + PM.OVERHEAD != recordSize) {
                throw new CorruptJournalException("Invalid record size " + recordSize + " for PM record at "
                        + addressToString(from, timestamp));
            }

            long address = from + PM.OVERHEAD;
            int index = 0;
            int loaded = 0;
            long lastPage = Long.MAX_VALUE;
            int lastVolumeHandle = Integer.MAX_VALUE;
            for (int remaining = count; remaining > 0; remaining--) {
                if (index == loaded) {
                    final int loadedSize = Math.min((_readBuffer.capacity() / PM.ENTRY_SIZE) * PM.ENTRY_SIZE, remaining
                            * PM.ENTRY_SIZE);
                    read(address, loadedSize);
                    address += loadedSize;
                    index = 0;
                    loaded = loadedSize / PM.ENTRY_SIZE;
                    if (loaded <= 0) {
                        throw new CorruptJournalException("Could not load PageMap segment in entry "
                                + (count - remaining + 1) + " at " + addressToString(from, timestamp));
                    }
                }
                final int volumeHandle = PM.getEntryVolumeHandle(_readBuffer, index);
                final long pageAddress = PM.getEntryPageAddress(_readBuffer, index);
                final long pageTimestamp = PM.getEntryTimestamp(_readBuffer, index);
                final long journalAddress = PM.getEntryJournalAddress(_readBuffer, index);
                if (_selectedPages.isSelected(pageAddress) && _selectedTimestamps.isSelected(pageTimestamp)) {
                    if (pageAddress != lastPage || volumeHandle != lastVolumeHandle) {
                        flush();
                        lastPage = pageAddress;
                        lastVolumeHandle = volumeHandle;
                        appendf("-- %5d:%,12d: ", volumeHandle, pageAddress);
                    }
                    appendf(" @%,d(%,d)", journalAddress, pageTimestamp);
                }
                index++;
            }
        }

        void dumpTransactionMap(final int count, final long from, final long timestamp, final int recordSize)
                throws PersistitIOException {
            if (count * TM.ENTRY_SIZE + TM.OVERHEAD != recordSize) {
                throw new CorruptJournalException("Invalid record size " + recordSize + " for TM record at "
                        + addressToString(from, timestamp));
            }
            long address = from + TM.OVERHEAD;
            int index = 0;
            int loaded = 0;
            for (int remaining = count; remaining > 0; remaining--) {
                if (index == loaded) {
                    read(address, Math.min(_readBuffer.capacity(), remaining * TM.ENTRY_SIZE));
                    address += _readBuffer.remaining();
                    index = 0;
                    loaded = _readBuffer.remaining() / TM.ENTRY_SIZE;
                    if (loaded <= 0) {
                        throw new CorruptJournalException("Could not load TransactionMap segment in entry "
                                + (count - remaining + 1) + " at " + addressToString(from, timestamp));
                    }
                }
                final long startTimestamp = TM.getEntryStartTimestamp(_readBuffer, index);
                final long commitTimestamp = TM.getEntryCommitTimestamp(_readBuffer, index);
                final long journalAddress = TM.getEntryJournalAddress(_readBuffer, index);
                final boolean isCommitted = commitTimestamp != 0;
                appendf("--  start %,12d  commit %,12d  @%,18d %s", startTimestamp, commitTimestamp, journalAddress,
                        isCommitted ? "committed" : "uncommitted");
                flush();
                index++;
            }
        }
    }
}
