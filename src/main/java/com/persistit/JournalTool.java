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
 * Created on Jan 25, 2005
 */
package com.persistit;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Stack;

import com.persistit.exception.PersistitException;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class JournalTool {

    private final static String[] ARGS_TEMPLATE = {
            "file|string:|Journal file name",
            "start|int:0:0:2000000000|Starting address in journal file",
            "end|int:2000000000:0:2000000000|Ending address in journal file",
            "startid|int:0:0:2000000000|starting id",
            "endid|int:2000000000:0:2000000000|ending id",
            "startet|int:0:0:2000000000|starting elapsed time",
            "endet|int:2000000000:0:2000000000|ending elapsed time",
            "_flag|u|Perform update operations",
            "_flag|d|Dump the transactions in readable form",
            "_flag|1|Dump overlapping transactions", "_flag|v|Verbose mode",
            "_flag|i|ID records only", };

    private final static SimpleDateFormat SDF = new SimpleDateFormat(
            "yyyy/MM/dd HH:mm:ss.SSS");

    private final Persistit _persistit;
    DataInputStream _is;
    String _path;
    long _startAddr;
    long _endAddr;
    long _startId;
    long _endId;
    int _createTreeCount = 0;
    int _removeTreeCount = 0;
    int _storeCount = 0;
    int _removeCount = 0;
    int _fetchCount = 0;
    int _traverseCount = 0;
    long _startElapsed = 0;
    long _endElapsed = 0;
    long _startTime = 0;
    String _flags;

    HashMap _exchangeMap = new HashMap();
    HashMap _treeMap = new HashMap();
    HashMap _threadMap = new HashMap();

    Stack _availableJournalRecords = new Stack();
    ArrayList _outstandingJournalRecords = new ArrayList();

    PrintStream _os = System.out;

    boolean _sequenceStarted = false;
    long _sequenceStartedElapsedTime;
    boolean _verbose = false;

    JournalTool(final Persistit persistit, final String[] args)
            throws IOException {
        _persistit = persistit;
        ArgParser ap = new ArgParser("com.persistit.JournalTools", args,
                ARGS_TEMPLATE);
        _path = ap.getStringValue("file");
        if (_path == null || _path.length() == 0) {
            ap.usage();
            return;
        }

        _startAddr = ap.getIntValue("start");
        _endAddr = ap.getIntValue("end");
        _startId = ap.getIntValue("startid");
        _endId = ap.getIntValue("endid");
        _startElapsed = ap.getIntValue("startet");
        _endElapsed = ap.getIntValue("endet");
        _flags = ap.getFlags();
        _verbose = _flags.indexOf('v') >= 0;

        _is = new DataInputStream(new BufferedInputStream(new FileInputStream(
                _path), 1024 * 256));
    }

    JournalRecord allocJournalRecord(int index) {
        if (_availableJournalRecords.isEmpty()) {
            return new JournalRecord(index);
        } else {
            JournalRecord jr = (JournalRecord) _availableJournalRecords.pop();
            jr.reset(index);
            return jr;
        }
    }

    /**
     * Replays the journal file.
     */
    void replay() {

        long displayedTime = 0;
        long previousAddr = 0;
        long addr = 0;
        int type = -1;
        try {
            StringBuffer sb = new StringBuffer();
            boolean done = false;
            for (int count = 0; !done; count++) {
                JournalRecord jr = allocJournalRecord(count);
                int size = jr.read(_is, addr);
                type = jr._type;
                if (type == -1)
                    break;

                done = addr > _endAddr || jr._id > _endId
                        || jr._elapsedTime >= _endElapsed;

                boolean include = !done && addr >= _startAddr
                        && jr._id >= _startId
                        && jr._elapsedTime >= _startElapsed;

                long time = jr._elapsedTime + _startTime;
                if (time - displayedTime > 5000) {
                    _os.flush();
                }

                if (_verbose && count % 10000 == 0) {
                    sb.setLength(0);
                    Util.fill(sb, count, 10);
                    sb.append(" addr: ");
                    Util.fill(sb, addr, 12);
                    sb.append(" elapsed:");
                    Util.fill(sb, jr._elapsedTime, 9);
                    sb.append(" store: ");
                    Util.fill(sb, _storeCount, 8);
                    sb.append(" remove: ");
                    Util.fill(sb, _removeCount, 8);
                    sb.append(" fetch: ");
                    Util.fill(sb, _fetchCount, 8);
                    sb.append(" traverse: ");
                    Util.fill(sb, _traverseCount, 8);
                    sb.append(" trees created: ");
                    Util.fill(sb, _createTreeCount, 5);
                    sb.append(" removed: ");
                    Util.fill(sb, _removeTreeCount, 5);
                    if (!include)
                        sb.append(" - skipped");
                    System.out.println(sb);
                }

                if (include) {
                    if (_verbose && time - displayedTime > 60000) {
                        System.out.println("Time at elapsed=" + jr._elapsedTime
                                + " is " + SDF.format(new Date(time)));
                        displayedTime = time;
                    }

                    if (_flags.indexOf('u') >= 0) {
                        jr.execute();
                    }
                    if (_flags.indexOf('d') >= 0) {
                        jr.dump();
                    }
                    if (_flags.indexOf('1') >= 0) {
                        jr.dumpOverlap();
                    }
                    if (_flags.indexOf('i') >= 0) {
                        if (jr._type == Journal.TREE_ID
                                || jr._type == Journal.THREAD_ID
                                || jr._type == Journal.START_TIME) {
                            jr.dump();
                        }
                    }
                    if (_flags.indexOf('t') >= 0) {
                        if (jr._type == Journal.CREATE_TREE
                                || jr._type == Journal.REMOVE_TREE) {
                            jr.dump();
                        }
                    }

                    // Do the work associated with this JournalRecord
                }

                if (type == Journal.COMPLETED) {
                    int outstandingCount = _outstandingJournalRecords.size();
                    for (int i = outstandingCount; --i >= 0;) {
                        JournalRecord jr2 = (JournalRecord) _outstandingJournalRecords
                                .get(i);
                        if (jr2._id == jr._id) {
                            _availableJournalRecords
                                    .push(_outstandingJournalRecords.remove(i));
                        }
                    }
                }
                if (jr.requiresCompletion()) {
                    if (_outstandingJournalRecords.size() >= 100) {
                        System.out.println("Culling 50 outstanding records:");
                        for (int i = 0; i < 50; i++) {
                            JournalRecord jr2 = (JournalRecord) _outstandingJournalRecords
                                    .remove(0);
                            jr2.dump();
                        }
                        System.out.println("End of culled records");
                    }
                    _outstandingJournalRecords.add(jr);
                } else {
                    _availableJournalRecords.push(jr);
                }
                previousAddr = addr;
                addr += size;

            }
        } catch (Exception ex) {
            System.out.println("Exception at addr=" + addr + " type=" + type
                    + " previousAddr=" + previousAddr);
            ex.printStackTrace();
        }
    }

    /**
     * Aggregates all of the information recorded in one Journal record.
     */
    class JournalRecord {
        int _type;
        int _index;
        long _id;
        long _addr;
        long _elapsedTime;
        int _threadHandle;
        int _treeHandle;
        String _treeName;
        long _volumeId;
        String _volumeName;
        long _incrementBy;
        boolean _fetchFirst;
        int _minBytes;
        boolean _deep;
        int _directionIndex;
        Key _key1 = new Key(_persistit);
        Key _key2 = new Key(_persistit);
        Value _value = new Value(_persistit);
        //
        // Resolved entities
        //
        Volume _volume;
        String _threadName;

        JournalRecord(int index) {
            _index = index;
        }

        boolean requiresCompletion() {
            return _type == Journal.STORE || _type == Journal.INCREMENT
                    || _type == Journal.FETCH || _type == Journal.TRAVERSE;
        }

        void reset(int index) {
            _type = 0;
            _index = index;
            _id = 0;
            _addr = 0;
            _elapsedTime = 0;
            _threadHandle = 0;
            _treeHandle = 0;
            _treeName = null;
            _volumeId = 0;
            _volumeName = null;
            _incrementBy = 0;
            _fetchFirst = false;
            _minBytes = 0;
            _deep = false;
            _directionIndex = 0;
            _key1.clear();
            _key2.clear();
            _value.clear();
        }

        int read(DataInputStream dis, long addr) throws IOException {
            _type = dis.read();
            if (_type == -1)
                return -1;

            _id = dis.readLong();
            _threadHandle = dis.readInt();
            _elapsedTime = dis.readInt();
            _addr = addr;

            int size = 1 + 8 + 4 + 4;

            switch (_type) {
            case Journal.COMPLETED: {
                verifyMarker(dis, addr);
                size++;
                break;
            }

            case Journal.START_TIME: {
                _startTime = dis.readLong();
                verifyMarker(dis, addr);
                size += 8 + 1;
                break;
            }

            case Journal.THREAD_ID: {
                _threadName = dis.readUTF();
                size += 2 + (_threadName.length()) + 1;
                verifyMarker(dis, addr);
                Integer threadId = new Integer(_threadHandle);
                _threadMap.put(threadId, _threadName);
                size++;
                break;
            }

            case Journal.TREE_ID: {
                int treeHandle = dis.readInt();
                long volumeId = dis.readLong();
                _volumeName = dis.readUTF();
                _treeHandle = dis.readInt();
                _treeName = dis.readUTF();
                Integer treeId = new Integer(_treeHandle);
                _treeMap.put(treeId, new String[] { _treeName, _volumeName });
                verifyMarker(dis, addr);
                size += 4 + 8 + (2 + _volumeName.length());
                size += 4 + (2 + _treeName.length()) + 1;
                break;
            }

            case Journal.CREATE_TREE: {
                long volumeId = dis.readLong();
                _volumeName = dis.readUTF();
                _treeHandle = dis.readInt();
                _treeName = dis.readUTF();
                Integer treeId = new Integer(_treeHandle);
                _treeMap.put(treeId, new String[] { _treeName, _volumeName });
                verifyMarker(dis, addr);
                size += 8 + (2 + _volumeName.length());
                size += 4 + (2 + _treeName.length()) + 1;
                break;
            }

            case Journal.REMOVE_TREE: {
                _treeHandle = dis.readInt();
                verifyMarker(dis, addr);
                size += 4 + 1;
                break;
            }

            case Journal.STORE: {
                _treeHandle = dis.readInt();
                int encodedKeySize = dis.readInt();
                dis.read(_key1.getEncodedBytes(), 0, encodedKeySize);
                _key1.setEncodedSize(encodedKeySize);
                int encodedValueSize = dis.readInt();
                _value.ensureFit(encodedValueSize);
                dis.read(_value.getEncodedBytes(), 0, encodedValueSize);
                _value.setEncodedSize(encodedValueSize);
                _fetchFirst = dis.readBoolean();
                verifyMarker(dis, addr);
                size += 4 + 4 + encodedKeySize + 4 + encodedValueSize + 1 + 1;
                break;
            }
            case Journal.INCREMENT: {
                _treeHandle = dis.readInt();
                int encodedKeySize = dis.readInt();
                dis.read(_key1.getEncodedBytes(), 0, encodedKeySize);
                _key1.setEncodedSize(encodedKeySize);
                _incrementBy = dis.readLong();
                int encodedValueSize = dis.readInt();
                _value.ensureFit(encodedValueSize);
                dis.read(_value.getEncodedBytes(), 0, encodedValueSize);
                _value.setEncodedSize(encodedValueSize);
                boolean fetchFirst = dis.readBoolean();
                verifyMarker(dis, addr);
                size += 4 + 4 + encodedKeySize + 4 + 8 + 1;
                break;
            }

            case Journal.REMOVE: {
                _treeHandle = dis.readInt();

                int encodedKeySize1 = dis.readInt();
                dis.read(_key1.getEncodedBytes(), 0, encodedKeySize1);
                _key1.setEncodedSize(encodedKeySize1);
                int encodedKeySize2 = dis.readInt();
                dis.read(_key2.getEncodedBytes(), 0, encodedKeySize2);
                _key2.setEncodedSize(encodedKeySize2);
                boolean fetchFirst = dis.readBoolean();
                verifyMarker(dis, addr);
                size += 4 + 4 + encodedKeySize1 + 4 + encodedKeySize2 + 1 + 1;
                break;
            }

            case Journal.FETCH: {
                _treeHandle = dis.readInt();

                int encodedKeySize = dis.readInt();
                dis.read(_key1.getEncodedBytes(), 0, encodedKeySize);
                _key1.setEncodedSize(encodedKeySize);
                _minBytes = dis.readInt();
                verifyMarker(dis, addr);
                size += 4 + 4 + encodedKeySize + 4 + 1;
                break;
            }

            case Journal.TRAVERSE: {
                _treeHandle = dis.readInt();
                int encodedKeySize = dis.readInt();
                dis.read(_key1.getEncodedBytes(), 0, encodedKeySize);
                _key1.setEncodedSize(encodedKeySize);

                _directionIndex = dis.readByte();
                _deep = dis.readBoolean();
                _minBytes = dis.readInt();

                verifyMarker(dis, addr);
                size += 4 + 4 + encodedKeySize + 1 + 1 + 4 + 1;
                break;
            }

            default:
                throw new RuntimeException("Invalid record type " + _type);
            }

            return size;

        }

        void execute() throws PersistitException {
            switch (_type) {
            case Journal.COMPLETED: {
                _outstandingJournalRecords.remove(this);
                break;
            }

            case Journal.START_TIME: {
                break;
            }

            case Journal.THREAD_ID: {
                break;
            }

            case Journal.TREE_ID: {
                break;
            }

            case Journal.CREATE_TREE: {
                Volume volume = _persistit.getVolume(_volumeName);
                if (volume != null) {
                    volume.getTree(_treeName, true);
                    _createTreeCount++;
                }
                break;
            }

            case Journal.REMOVE_TREE: {
                Volume volume = _persistit.getVolume(_volumeName);
                if (volume != null) {
                    if (volume.removeTree(_treeName)) {
                        _removeTreeCount++;
                    }
                }
                break;
            }

            case Journal.STORE: {
                Exchange exchange = exchange(_treeHandle);
                if (exchange != null && !exchange.isDirectoryExchange()) {
                    _key1.copyTo(exchange.getKey());
                    _value.copyTo(exchange.getValue());
                    if (_fetchFirst) {
                        exchange.fetchAndStore();
                    } else {
                        exchange.store();
                    }
                    _storeCount++;
                }
                break;
            }
            case Journal.INCREMENT: {
                Exchange exchange = exchange(_treeHandle);
                if (exchange != null && !exchange.isDirectoryExchange()) {
                    _key1.copyTo(exchange.getKey());
                    _value.copyTo(exchange.getValue());
                    exchange.getValue().armAtomicIncrement(_incrementBy);
                    exchange.store();
                    _storeCount++;
                }
                break;
            }

            case Journal.REMOVE: {
                Exchange exchange = exchange(_treeHandle);
                if (exchange != null && !exchange.isDirectoryExchange()) {
                    _key1.copyTo(exchange.getAuxiliaryKey1());
                    _key2.copyTo(exchange.getAuxiliaryKey2());
                    exchange.getAuxiliaryKey1().copyTo(exchange.getKey());
                    exchange.removeKeyRangeInternal(
                            exchange.getAuxiliaryKey1(), exchange
                                    .getAuxiliaryKey2(), _fetchFirst);
                    _removeCount++;
                }
                break;
            }

            case Journal.FETCH: {
                Exchange exchange = exchange(_treeHandle);
                if (exchange != null) {
                    _key1.copyTo(exchange.getAuxiliaryKey1());
                    exchange.fetch(_minBytes);
                    _fetchCount++;
                }
                break;
            }

            case Journal.TRAVERSE: {
                Exchange exchange = exchange(_treeHandle);
                if (exchange != null) {
                    _key1.copyTo(exchange.getAuxiliaryKey1());
                    exchange.traverse(Key.DIRECTIONS[_directionIndex], _deep,
                            _minBytes);
                    _traverseCount++;
                }
                break;
            }
            }
        }

        public void dump() {
            _os.println(toString());
        }

        public void dumpOverlap() {
            if (_sequenceStarted
                    || _outstandingJournalRecords.size() > 1
                    || _outstandingJournalRecords.size() == 1
                    && (_type != Journal.COMPLETED || ((JournalRecord) _outstandingJournalRecords
                            .get(0))._id != _id)) {
                if (!_sequenceStarted) {
                    _sequenceStarted = true;
                    _sequenceStartedElapsedTime = _elapsedTime;
                    _os.println();
                    _os.print("--outstanding:");
                    for (int i = 0; i < _outstandingJournalRecords.size(); i++) {
                        JournalRecord jr = (JournalRecord) _outstandingJournalRecords
                                .get(i);
                        _os.print(" ");
                        _os.print(jr._id);
                    }
                    _os.println();
                    for (int i = 0; i < _outstandingJournalRecords.size(); i++) {
                        JournalRecord jr = (JournalRecord) _outstandingJournalRecords
                                .get(i);
                        _os.println(jr.toString());
                    }
                }
                _os.println(toString());
            } else {
                if (_outstandingJournalRecords.isEmpty()) {
                    _sequenceStarted = false;
                }
            }
            if (_sequenceStarted
                    && _elapsedTime - _sequenceStartedElapsedTime > 5000) {
                _sequenceStarted = false;
                _os.println("--Stopped printing overlapping records");
            }
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            toString(sb);
            return sb.toString();
        }

        public void toString(StringBuffer sb) {
            Util.fill(sb, _index, 9);
            sb.append(": addr=");
            Util.fill(sb, _addr, 9);
            sb.append(" id=");
            Util.fill(sb, _id, 8);
            sb.append(" elapsed=");
            Util.fill(sb, _elapsedTime, 9);
            sb.append(" threadId=");
            Util.fill(sb, _threadHandle, 4);
            sb.append(" type=");
            Util.fill(sb, _type, 2);
            switch (_type) {
            case Journal.COMPLETED: {
                sb.append(" COMPLETED id=");
                sb.append(_id);
                break;
            }

            case Journal.START_TIME: {
                sb.append(" START TIME time=");
                sb.append(_startTime);
                sb.append(" @ ");
                sb.append(SDF.format(new Date(_startTime)));
                break;
            }

            case Journal.THREAD_ID: {
                sb.append(" THREAD_ID name=");
                sb.append(_threadName);
                break;
            }

            case Journal.TREE_ID: {
                sb.append(" TREE_ID id=");
                sb.append(_treeHandle);
                sb.append(" treeName=");
                sb.append(_treeName);
                sb.append(" in volume ");
                sb.append(_volumeName);
                break;
            }

            case Journal.CREATE_TREE: {
                sb.append(" CREATE_TREE id=");
                sb.append(_treeHandle);
                sb.append(" treeName=");
                sb.append(_treeName);
                sb.append(" in volume ");
                sb.append(_volumeName);
                _createTreeCount++;
                break;
            }

            case Journal.REMOVE_TREE: {
                sb.append(" REMOVE_TREE id=");
                sb.append(_treeHandle);
                _removeTreeCount++;
                break;
            }

            case Journal.STORE: {
                sb.append(" STORE treeId=");
                sb.append(_treeHandle);
                sb.append(" ");
                sb.append(_key1.toString());
                sb.append(" fetchFirst=");
                sb.append(_fetchFirst);
                sb.append(" minBytes=");
                sb.append(_minBytes);
                sb.append(" valueLength=");
                sb.append(_value.getEncodedSize());
                _storeCount++;
                break;
            }

            case Journal.INCREMENT: {
                sb.append(" INCREMENT treeId=");
                sb.append(_treeHandle);
                sb.append(" ");
                sb.append(_key1.toString());
                sb.append(" incrementBy=");
                sb.append(_incrementBy);
                _storeCount++;
                break;
            }

            case Journal.REMOVE: {
                sb.append(" REMOVE treeId=");
                sb.append(_treeHandle);
                sb.append(" ");
                sb.append(_key1.toString());
                sb.append("-->");
                sb.append(_key2.toString());
                sb.append(" fetchFirst=");
                sb.append(_fetchFirst);
                sb.append(" minBytes=");
                sb.append(_minBytes);
                _removeCount++;
                break;
            }

            case Journal.FETCH: {
                sb.append(" FETCH treeId=");
                sb.append(_treeHandle);
                sb.append(" ");
                sb.append(_key1.toString());
                sb.append(" minBytes=");
                sb.append(_minBytes);
                _fetchCount++;
                break;
            }

            case Journal.TRAVERSE: {
                sb.append(" TRAVERSE treeId=");
                sb.append(_treeHandle);
                sb.append(" ");
                sb.append(_key1.toString());
                sb.append(" minBytes=");
                sb.append(_minBytes);
                _traverseCount++;
                break;
            }
            }

        }

        private void verifyMarker(DataInputStream dis, long addr)
                throws IOException {
            int marker = dis.readByte();
            if (marker != Journal.END_MARKER) {
                throw new RuntimeException("Invalid marker byte " + marker
                        + " at " + addr);
            }
        }
    }

    private Exchange exchange(int treeHandle) throws PersistitException {
        Integer treeId = new Integer(treeHandle);
        Exchange exchange = (Exchange) _exchangeMap.get(treeId);
        if (exchange != null)
            return exchange;

        String[] treeInfo = (String[]) _treeMap.get(treeId);
        if (treeInfo == null) {
            return null;
        } else {
            exchange = _persistit.getExchange(treeInfo[1], treeInfo[0], false);
            _exchangeMap.put(treeId, exchange);
        }
        return exchange;
    }

    public static void main(String[] args) throws Exception {
        final Persistit persistit = new Persistit();
        JournalTool jt = new JournalTool(persistit, args);
        try {
            if (jt._flags.indexOf('u') >= 0) {
                persistit.initialize();
            }
            jt.replay();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            persistit.close();
        }

    }
}
