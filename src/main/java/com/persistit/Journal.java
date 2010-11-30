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
 * Created on Oct 3, 2004
 */
package com.persistit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Very temporary implementation of journaling for debugging a problem in
 * Clarity.
 * 
 * 
 * @version 1.0
 */
class Journal {
    public final static int JOURNAL_BUFFER_SIZE = 1024 * 1024;

    public final static int START_TIME = 60;
    public final static int THREAD_ID = 61;
    public final static int END_MARKER = 63;
    public final static int COMPLETED = 64;
    public final static int TREE_ID = 65;
    public final static int CREATE_TREE = 66;
    public final static int REMOVE_TREE = 67;
    public final static int STORE = 68;
    public final static int INCREMENT = 69;
    public final static int REMOVE = 70;
    public final static int FETCH = 71;
    public final static int TRAVERSE = 72;

    private long _counter;
    private FileOutputStream _fos;
    private DataOutputStream _os;
    private boolean _enabled = false;
    private boolean _enabledFetches = false;
    private Hashtable _treeHashTable = new Hashtable();
    private Hashtable _threadHashTable = new Hashtable();
    int _maxTreeHandle = 0;
    int _maxThreadHandle = 0;
    final private Persistit _persistit;

    Journal(final Persistit persistit) {
        _persistit = persistit;
    }

    synchronized void setup(String path, boolean enableFetchOperations) {
        try {
            if (_os != null) {
                _os.close();
                _fos.close();
                _os = null;
                _fos = null;
                _treeHashTable.clear();
                _enabled = false;
                _enabledFetches = false;
            }
            if (path != null) {
                _fos = new FileOutputStream(path, false);
                _os = new DataOutputStream(new BufferedOutputStream(_fos,
                        JOURNAL_BUFFER_SIZE));
                _enabled = true;
                _enabledFetches = enableFetchOperations;

                Thread flusher = new Thread(new Runnable() {
                    public void run() {
                        for (;;) {
                            try {
                                Thread.sleep(10000);
                                DataOutputStream os = _os;
                                if (os == null)
                                    break;
                                os.flush();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                flusher.setDaemon(true);
                flusher.start();
                long id = _counter++;
                int threadHandle = threadHandle();
                _os.writeByte(START_TIME);
                _os.writeLong(id);
                _os.writeInt(threadHandle);
                _os.writeInt((int) _persistit.elapsedTime());
                _os.writeLong(_persistit.startTime());
                _os.writeByte(END_MARKER);
            }
        } catch (IOException ioe) {
            // TODO
            ioe.printStackTrace();
        }
    }

    synchronized void close() {
        try {
            if (_os != null) {
                _os.close();
                _fos.close();
                _os = null;
                _fos = null;
                _treeHashTable.clear();
                _enabled = false;
                _enabledFetches = false;
            }
        } catch (IOException ioe) {
            // TODO
            ioe.printStackTrace();
        }
    }

    void flush() {
        try {
            if (_os != null)
                _os.flush();
        } catch (IOException ioe) {
            // TODO
            ioe.printStackTrace();
        }
    }

    void sync() {
        try {
            if (_fos != null)
                _fos.getFD().sync();
        } catch (IOException ioe) {
            // TODO
            ioe.printStackTrace();
        }
    }

    long beginStore(Tree tree, Key key, Value value, boolean fetchFirst) {
        long id = -1;
        if (_enabled) {
            try {
                synchronized (this) {
                    id = _counter++;
                    int threadHandle = threadHandle();
                    int treeHandle = treeHandle(tree);
                    if (value.isAtomicIncrementArmed()) {
                        _os.writeByte(INCREMENT);
                        _os.writeLong(id);
                        _os.writeInt(threadHandle);
                        _os.writeInt((int) _persistit.elapsedTime());
                        _os.writeInt(treeHandle);
                        _os.writeInt(key.getEncodedSize());
                        _os.write(key.getEncodedBytes(), 0,
                                key.getEncodedSize());
                        _os.writeLong(value.getAtomicIncrementValue());
                        _os.writeInt(value.getEncodedSize());
                        _os.write(value.getEncodedBytes(), 0,
                                value.getEncodedSize());
                        _os.writeBoolean(fetchFirst);
                        _os.writeByte(END_MARKER);
                    } else {
                        _os.writeByte(STORE);
                        _os.writeLong(id);
                        _os.writeInt(threadHandle);
                        _os.writeInt((int) _persistit.elapsedTime());
                        _os.writeInt(treeHandle);
                        _os.writeInt(key.getEncodedSize());
                        _os.write(key.getEncodedBytes(), 0,
                                key.getEncodedSize());
                        _os.writeInt(value.getEncodedSize());
                        _os.write(value.getEncodedBytes(), 0,
                                value.getEncodedSize());
                        _os.writeBoolean(fetchFirst);
                        _os.writeByte(END_MARKER);
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return id;
    }

    long beginRemove(Tree tree, Key key1, Key key2, boolean fetchFirst) {

        long id = -1;
        if (_enabled) {
            try {
                synchronized (this) {
                    id = _counter++;
                    int threadHandle = threadHandle();
                    int treeHandle = treeHandle(tree);

                    _os.writeByte(REMOVE);
                    _os.writeLong(id);
                    _os.writeInt(threadHandle);
                    _os.writeInt((int) _persistit.elapsedTime());
                    _os.writeInt(treeHandle);
                    _os.writeInt(key1.getEncodedSize());
                    _os.write(key1.getEncodedBytes(), 0, key1.getEncodedSize());
                    _os.writeInt(key2.getEncodedSize());
                    _os.write(key2.getEncodedBytes(), 0, key2.getEncodedSize());
                    _os.writeBoolean(fetchFirst);
                    _os.writeByte(END_MARKER);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return id;
    }

    long beginCreateTree(Volume volume, String treeName, int treeIndex) {

        long id = -1;
        if (_enabled) {
            try {
                synchronized (this) {
                    id = _counter++;
                    int threadHandle = threadHandle();

                    _os.writeByte(CREATE_TREE);
                    _os.writeLong(id);
                    _os.writeInt(threadHandle);
                    _os.writeInt((int) _persistit.elapsedTime());
                    _os.writeLong(volume.getId());
                    _os.writeUTF(volume.getPath());
                    _os.writeInt(treeIndex);
                    _os.writeUTF(treeName);
                    _os.writeByte(END_MARKER);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return id;
    }

    long beginRemoveTree(Tree tree) {

        long id = -1;
        if (_enabled) {
            try {
                synchronized (this) {
                    id = _counter++;
                    int threadHandle = threadHandle();
                    int treeHandle = treeHandle(tree);

                    _os.writeByte(REMOVE_TREE);
                    _os.writeLong(id);
                    _os.writeInt(threadHandle);
                    _os.writeInt((int) _persistit.elapsedTime());
                    _os.writeInt(treeHandle);
                    _os.writeByte(END_MARKER);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return id;
    }

    // synchronized long beginFetch(
    // Tree tree,
    // Key key,
    // int minBytes)
    // {
    // long id = -1;
    // if (_enabledFetches)
    // {
    // try
    // {
    // id = _counter++;
    // int threadHandle = threadHandle();
    // int treeHandle = treeHandle(tree);
    // _os.writeByte(FETCH);
    // _os.writeLong(id);
    // _os.writeInt(threadHandle);
    // _os.writeInt((int)Persistit.elapsedTime());
    // _os.writeInt(treeHandle);
    // _os.writeInt(key.getEncodedSize());
    // _os.write(key.getEncodedBytes(), 0, key.getEncodedSize());
    // _os.writeInt(minBytes);
    // _os.writeByte(END_MARKER);
    // }
    // catch (IOException ioe)
    // {
    // ioe.printStackTrace();
    // }
    // }
    // return id;
    // }
    //
    // synchronized long beginTraverse(
    // Tree tree,
    // Key key,
    // Key.Direction direction,
    // boolean deep,
    // int minBytes )
    // {
    // long id = -1;
    // if (_enabledFetches)
    // {
    // try
    // {
    // id = _counter++;
    // int threadHandle = threadHandle();
    // int treeHandle = treeHandle(tree);
    // _os.writeByte(TRAVERSE);
    // _os.writeLong(id);
    // _os.writeInt(threadHandle);
    // _os.writeInt((int)Persistit.elapsedTime());
    // _os.writeInt(treeHandle);
    // _os.writeInt(key.getEncodedSize());
    // _os.write(key.getEncodedBytes(), 0, key.getEncodedSize());
    // _os.writeByte(direction.getIndex());
    // _os.writeBoolean(deep);
    // _os.writeInt(minBytes);
    // _os.writeByte(END_MARKER);
    // }
    // catch (IOException ioe)
    // {
    // ioe.printStackTrace();
    // }
    // }
    // return id;
    // }
    //
    synchronized void completed(long id) {
        if (id > -1 && _enabled) {
            try {
                int threadHandle = threadHandle();
                _os.writeByte(COMPLETED);
                _os.writeLong(id);
                _os.writeInt(threadHandle);
                _os.writeInt((int) _persistit.elapsedTime());
                _os.writeByte(END_MARKER);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private int treeHandle(Tree tree) throws IOException {
        Integer handleValue = (Integer) _treeHashTable.get(tree);
        if (handleValue == null) {
            int treeHandle = ++_maxTreeHandle;
            int threadHandle = threadHandle();
            _os.writeByte(TREE_ID);
            _os.writeLong(_counter);
            _os.writeInt(threadHandle);
            _os.writeInt((int) _persistit.elapsedTime());
            _os.writeInt(treeHandle);
            _os.writeLong(tree.getVolume().getId());
            _os.writeUTF(tree.getVolume().getPath());
            _os.writeInt(tree.getTreeIndex());
            _os.writeUTF(tree.getName());
            _os.writeByte(END_MARKER);

            handleValue = new Integer(treeHandle);
            _treeHashTable.put(tree, handleValue);
            return treeHandle;
        }
        return handleValue.intValue();
    }

    private int threadHandle() throws IOException {
        String threadName = Thread.currentThread().getName();
        Integer handleValue = (Integer) _threadHashTable.get(threadName);
        if (handleValue == null) {
            int threadHandle = ++_maxThreadHandle;
            _os.writeByte(THREAD_ID);
            _os.writeLong(_counter);
            _os.writeInt(threadHandle);
            _os.writeInt((int) _persistit.elapsedTime());
            _os.writeUTF(threadName);
            _os.writeByte(END_MARKER);

            handleValue = new Integer(threadHandle);
            _threadHashTable.put(threadName, handleValue);
            return threadHandle;
        }
        return handleValue.intValue();
    }

    /**
     * Replay the journal
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String path = args[0];

        DataInputStream is = new DataInputStream(new BufferedInputStream(
                new FileInputStream(path), 1024 * 1024));

        Hashtable exchangeTable = new Hashtable();
        Hashtable threadTable = new Hashtable();

        long addr = 0;
        final Persistit persistit = new Persistit();
        persistit.initialize();
        System.out.println();
        System.out.println("Replaying journal file " + path);
        System.out.println();
        int type = -1;
        int lastType = -1;

        long lastAddr = -1;
        long lastAddr2 = -1;

        long lastId = -1;
        int lastThreadHandle = -1;

        try {
            int createTreeCount = 0;
            int removeTreeCount = 0;
            int storeCount = 0;
            int removeCount = 0;
            int fetchCount = 0;
            int traverseCount = 0;
            long startTime = 0;
            StringBuilder sb = new StringBuilder();
            for (int count = 0;; count++) {
                lastType = type;
                lastAddr2 = lastAddr;
                lastAddr = addr;

                type = is.read();
                if (type == -1)
                    break;

                long id = is.readLong();
                int threadHandle = is.readInt();
                int elapsedTime = is.readInt();
                addr += 17;

                if (count % 100000 == 0) {
                    sb.setLength(0);
                    Util.fill(sb, count, 10);
                    sb.append(" addr: ");
                    Util.fill(sb, addr, 12);
                    sb.append(" store: ");
                    Util.fill(sb, storeCount, 8);
                    sb.append(" remove: ");
                    Util.fill(sb, removeCount, 8);
                    sb.append(" fetch: ");
                    Util.fill(sb, fetchCount, 8);
                    sb.append(" traverse: ");
                    Util.fill(sb, traverseCount, 8);
                    sb.append(" trees created: ");
                    Util.fill(sb, createTreeCount, 5);
                    sb.append(" removed: ");
                    Util.fill(sb, removeTreeCount, 5);
                    sb.append(" elapsed log time:");
                    Util.fill(sb, elapsedTime, 9);
                    System.out.println(sb);

                }
                switch (type) {
                case COMPLETED: {
                    verifyMarker(is, addr);
                    addr += 8 + 1;
                    lastId = -1;
                    break;
                }

                case START_TIME: {
                    startTime = is.readLong();
                    verifyMarker(is, addr);
                    addr += 8 + 1;
                    break;
                }

                case THREAD_ID: {
                    String threadName = is.readUTF();
                    verifyMarker(is, addr);
                    addr += 2 + (threadName.length()) + 1;
                    Integer threadId = new Integer(threadHandle);
                    threadTable.put(threadName, threadId);
                    threadTable.put(threadId, threadName);
                    break;
                }

                case TREE_ID: {
                    int treeHandle = is.readInt();
                    long volumeId = is.readLong();
                    String volumeName = is.readUTF();
                    int treeIndex = is.readInt();
                    String treeName = is.readUTF();
                    verifyMarker(is, addr);
                    addr += 4 + 8 + (2 + volumeName.length()) + 4
                            + (2 + treeName.length()) + 1;

                    Volume volume = persistit
                            .getVolume(fixupVolumeName(volumeName));
                    Exchange exchange;
                    if (Volume.DIRECTORY_TREE_NAME.equals(treeName)) {
                        exchange = volume.directoryExchange();
                    } else {
                        exchange = persistit
                                .getExchange(volume, treeName, true);
                    }
                    exchangeTable.put(new Integer(treeHandle), exchange);
                    break;
                }

                case CREATE_TREE: {
                    long volumeId = is.readLong();
                    String volumeName = is.readUTF();
                    int treeIndex = is.readInt();
                    String treeName = is.readUTF();
                    verifyMarker(is, addr);
                    addr += 8 + (2 + volumeName.length()) + 4
                            + (2 + treeName.length()) + 1;
                    Volume volume = persistit
                            .getVolume(fixupVolumeName(volumeName));
                    Tree tree = volume.getTree(treeName, true);
                    createTreeCount++;
                    break;
                }

                case REMOVE_TREE: {
                    Exchange exchange = exchange(is, addr, exchangeTable);
                    verifyMarker(is, addr);
                    addr += 4 + 1;
                    if (!exchange.isDirectoryExchange()) {
                        exchange.removeTree();
                        removeTreeCount++;
                    }
                    break;
                }

                case STORE: {
                    if (lastId != -1) {
                        System.out.println("STORE "
                                + "Overlap at addr="
                                + lastAddr
                                + " id="
                                + id
                                + " lastId = "
                                + lastId
                                + " lastAddr="
                                + lastAddr2
                                + " last thread="
                                + threadTable
                                        .get(new Integer(lastThreadHandle))
                                + " this thread="
                                + threadTable.get(new Integer(threadHandle)));
                    }
                    lastId = id;
                    lastThreadHandle = threadHandle;

                    Exchange exchange = exchange(is, addr, exchangeTable);
                    int encodedKeySize = is.readInt();
                    is.read(exchange.getKey().getEncodedBytes(), 0,
                            encodedKeySize);
                    exchange.getKey().setEncodedSize(encodedKeySize);
                    int encodedValueSize = is.readInt();
                    exchange.getValue().ensureFit(encodedValueSize);
                    is.read(exchange.getValue().getEncodedBytes(), 0,
                            encodedValueSize);
                    exchange.getValue().setEncodedSize(encodedValueSize);
                    boolean fetchFirst = is.readBoolean();
                    verifyMarker(is, addr);
                    addr += 4 + 4 + encodedKeySize + 4 + encodedValueSize + 1
                            + 1;
                    if (!exchange.isDirectoryExchange()) {
                        if (fetchFirst)
                            exchange.fetchAndStore();
                        else
                            exchange.store();
                        storeCount++;
                    }
                    break;
                }

                case INCREMENT: {
                    Exchange exchange = exchange(is, addr, exchangeTable);
                    if (lastId != -1) {
                        System.out.println("INCREMENT "
                                + "Overlap at addr="
                                + lastAddr
                                + " id="
                                + id
                                + " lastId = "
                                + lastId
                                + " lastAddr="
                                + lastAddr2
                                + " last thread="
                                + threadTable
                                        .get(new Integer(lastThreadHandle))
                                + " this thread="
                                + threadTable.get(new Integer(threadHandle)));
                    }
                    lastId = id;
                    lastThreadHandle = threadHandle;

                    int encodedKeySize = is.readInt();
                    is.read(exchange.getKey().getEncodedBytes(), 0,
                            encodedKeySize);
                    exchange.getKey().setEncodedSize(encodedKeySize);
                    long incrementBy = is.readLong();
                    int encodedValueSize = is.readInt();
                    exchange.getValue().ensureFit(encodedValueSize);
                    is.read(exchange.getValue().getEncodedBytes(), 0,
                            encodedValueSize);
                    exchange.getValue().setEncodedSize(encodedValueSize);
                    boolean fetchFirst = is.readBoolean();
                    verifyMarker(is, addr);
                    addr += 4 + 4 + encodedKeySize + 4 + 8 + 1;
                    if (!exchange.isDirectoryExchange()) {
                        exchange.getValue().armAtomicIncrement(incrementBy);
                        exchange.store();
                        storeCount++;
                    }
                    break;
                }

                case REMOVE: {
                    if (lastId != -1) {
                        System.out.println("REMOVE "
                                + "Overlap at addr="
                                + lastAddr
                                + " id="
                                + id
                                + " lastId = "
                                + lastId
                                + " lastAddr="
                                + lastAddr2
                                + " last thread="
                                + threadTable
                                        .get(new Integer(lastThreadHandle))
                                + " this thread="
                                + threadTable.get(new Integer(threadHandle)));
                    }
                    lastId = id;
                    lastThreadHandle = threadHandle;

                    Exchange exchange = exchange(is, addr, exchangeTable);
                    int encodedKeySize1 = is.readInt();
                    is.read(exchange.getAuxiliaryKey1().getEncodedBytes(), 0,
                            encodedKeySize1);
                    exchange.getAuxiliaryKey1().setEncodedSize(encodedKeySize1);
                    int encodedKeySize2 = is.readInt();
                    is.read(exchange.getAuxiliaryKey2().getEncodedBytes(), 0,
                            encodedKeySize2);
                    exchange.getAuxiliaryKey2().setEncodedSize(encodedKeySize2);
                    boolean fetchFirst = is.readBoolean();
                    verifyMarker(is, addr);
                    addr += 4 + 4 + encodedKeySize1 + 4 + encodedKeySize2 + 1
                            + 1;
                    Debug.debug1(id == 552818);
                    if (!exchange.isDirectoryExchange()) {
                        exchange.getAuxiliaryKey1().copyTo(exchange.getKey());
                        exchange.removeKeyRangeInternal(
                                exchange.getAuxiliaryKey1(),
                                exchange.getAuxiliaryKey2(), fetchFirst);
                        removeCount++;
                    }
                    break;
                }

                case FETCH: {
                    Exchange exchange = exchange(is, addr, exchangeTable);
                    int encodedKeySize = is.readInt();
                    is.read(exchange.getKey().getEncodedBytes(), 0,
                            encodedKeySize);
                    exchange.getKey().setEncodedSize(encodedKeySize);
                    int minBytes = is.readInt();
                    verifyMarker(is, addr);
                    addr += 4 + 4 + encodedKeySize + 4 + 1;
                    exchange.fetch(minBytes);
                    fetchCount++;
                    break;
                }

                case TRAVERSE: {
                    Exchange exchange = exchange(is, addr, exchangeTable);
                    int encodedKeySize = is.readInt();
                    is.read(exchange.getKey().getEncodedBytes(), 0,
                            encodedKeySize);
                    exchange.getKey().setEncodedSize(encodedKeySize);

                    int directionIndex = is.readByte();
                    boolean deep = is.readBoolean();
                    int minBytes = is.readInt();

                    verifyMarker(is, addr);
                    addr += 4 + 4 + encodedKeySize + 1 + 1 + 4 + 1;

                    Key.Direction direction = Key.DIRECTIONS[directionIndex];
                    exchange.traverse(direction, deep, minBytes);
                    traverseCount++;
                    break;
                }

                default:
                    throw new RuntimeException("Invalid record type " + type
                            + " at " + addr + " lastType=" + lastType
                            + " lastAddr=" + lastAddr + " lastAddr2="
                            + lastAddr2);
                }
            }
        } catch (Exception ex) {
            System.out.println("Exception at addr=" + addr + " type=" + type
                    + " at " + addr + " lastAddr=" + lastAddr + " lastAddr2="
                    + lastAddr2);
            ex.printStackTrace();
        } finally {
            is.close();
            persistit.close();
        }
    }

    private static void verifyMarker(DataInputStream is, long addr)
            throws IOException {
        int marker = is.readByte();
        if (marker != END_MARKER) {
            throw new RuntimeException("Invalid marker byte " + marker + " at "
                    + addr);
        }
    }

    private static Exchange exchange(DataInputStream is, long addr,
            Hashtable exchangeTable) throws IOException {
        int treeHandle = is.readInt();
        Exchange exchange = (Exchange) exchangeTable
                .get(new Integer(treeHandle));
        if (exchange == null) {
            throw new RuntimeException("No exchange for handle " + treeHandle
                    + " at " + addr);
        }
        return exchange;
    }

    private static String fixupVolumeName(String s) {
        int p = s.lastIndexOf('/');
        int q = s.lastIndexOf('\\');
        if (p > 0 && p < s.length() - 1 && p > q)
            return s.substring(p + 1);
        if (q > 0 && q < s.length())
            return s.substring(q + 1);
        return s;
    }
}
