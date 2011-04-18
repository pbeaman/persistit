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

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.PersistitException;

/**
 * <p>
 * Exposes information about the Persistit environment. With this public API,
 * embedding applications can query performance metrics and resources within
 * Persistit that are not exposed by the normal access methods. For example,
 * this class provides methods that enumerate the volumes currently mounted, the
 * size and effectiveness of buffer pool components, and many other items that
 * may be useful in tuning and managing Persistit.
 * </p>
 * <p>
 * This class is not a JMX MBean because in many cases a deployment will not
 * include the JMX API libraies, and in some applications it may not be
 * desirable to expose the inner workings of Persistit via JMX even when it is
 * available. A companion Open MBean that exposes the same information as this
 * class is provided in the {com.persistit.jmx} package.
 * </p>
 * <p>
 * All information returned by methods of this class represent a snapshot of
 * system state. Data structures such as
 * {@link com.persistit.Management.BufferPoolInfo} represent system state at the
 * time of that snapshot and are not kept updated as over time. Management
 * applications that display system state continuously should periodically call
 * methods of this class to get updated values.
 * </p>
 * 
 * @version 1.0
 */
class ManagementImpl implements Management {

    private final static long MAX_STALE = 5000;

    private static boolean _localRegistryCreated;
    private static long _taskIdCounter;

    private transient Persistit _persistit;
    private transient DisplayFilter _displayFilter;

    private boolean _registered = false;
    private String _registeredHostName;
    private HashMap<Long, Task> _tasks = new HashMap<Long, Task>();

    private TransactionInfo _transactionInfoCache = new TransactionInfo();

    public ManagementImpl(Persistit persistit) {
        _persistit = persistit;
        _displayFilter = new DisplayFilter() {

            @Override
            public String toKeyDisplayString(Exchange exchange) {
                return exchange.getKey().toString();
            }

            @Override
            public String toValueDisplayString(Exchange exchange) {
                return exchange.getValue().toString();
            }
        };
    }

    /**
     * Indicates whether Persistit is currently in the initialized state.
     * 
     * @return The state
     */
    public boolean isInitialized() {
        return _persistit.isInitialized();
    }

    /**
     * Returns the version name of the current Peristit instance.
     * 
     * @return the version name
     */
    public String getVersion() {
        return Persistit.version();
    }

    /**
     * Returns the copyright notice for the current Persistit instance.
     * 
     * @return the copyright notice
     */
    public String getCopyright() {
        return Persistit.copyright();
    }

    /**
     * Returns the system time at which Persistit was initialized.
     * 
     * @return start time, in milliseconds since January 1, 1970 00:00:00 GMT.
     */
    public long getStartTime() {
        return _persistit.startTime();
    }

    /**
     * Returns the elapsed time since startup in milliseconds
     * 
     * @return elapsed time in milliseconds
     */
    public long getElapsedTime() {
        return _persistit.elapsedTime();
    }

    @Override
    public long getCommittedTransactionCount() {
        return getTransactionInfo().getCommitCount();
    }

    @Override
    public long getRollbackCount() {
        return getTransactionInfo().getRollbackCount();
    }

    /**
     * Indicates whether Persistit will suspend its shutdown activities on
     * invocation of {@link #close}. This flag is intended for use by management
     * tools that need to keep Persistit open even when the application has
     * requested it to close so that the final state of the Persistit
     * environment can be examined.
     * 
     * @return <code>true</code> if Persistit will wait when attempting to
     *         close; <code>false</code> if the <code>close</code> operation
     *         will not be suspended.
     */
    public boolean isShutdownSuspended() {
        return _persistit.isShutdownSuspended();
    }

    /**
     * Determines whether Persistit will suspend its shutdown activities on
     * invocation of {@link #close}. This flag is intended for use by management
     * tools that need to keep Persistit open even when the application has
     * requested it to close so that the final state of the Persistit
     * environment can be examined.
     * 
     * @param enabled
     *            <code>true</code> to specify that Persistit will wait when
     *            attempting to close; otherwise <code>false</code>.
     */
    public void setShutdownSuspended(boolean suspended) {
        _persistit.setShutdownSuspended(suspended);
    }

    /**
     * Indicates whether Persistit is suspending all update operations. When
     * enabled, Persistit will indefinitely delay each Thread that attempts to
     * perform an update operation.
     * 
     * @return <code>true</code> if Persistit will suspend any attempt to update
     *         a <code>Volume</code>; otherwise <code>false</code>.
     * @throws RemoteException
     */
    public boolean isUpdateSuspended() {
        return _persistit.isUpdateSuspended();
    }

    /**
     * Controls whether Persistit will suspend all update operations. When
     * enabled, Persistit will delay each Thread that attempts to perform an
     * update operation indefinitely.
     * 
     * @param suspended
     * @throws RemoteException
     */
    public void setUpdateSuspended(boolean suspended) {
        _persistit.setUpdateSuspended(suspended);
    }

    /**
     * 
     */
    public void setIoLogFile(final String path) throws RemoteException {
        try {
            if (path == null || path.isEmpty()) {
                _persistit.getIOMeter().setLogFile(null);
            } else {
                _persistit.getIOMeter().setLogFile(path);
            }
        } catch (IOException e) {
            throw new WrappedRemoteException(e);
        }
    }

    /**
     * Controls whether Persistit will suspend the thread that copies pages from
     * the journal back to their respective Volumes. This flag is used by tools
     * that provide on-line backup.
     * 
     * @param suspended
     *            <code>true</code> to specify that Persistit will suspend
     *            journal copying; otherwise <code>false</code>.
     */
    public void setAppendOnly(boolean suspended) {
        _persistit.getJournalManager().setAppendOnly(suspended);
    }

    /**
     * Controls whether Persistit copies page from the journal back to their
     * volumes as fast as possible. Copying consumes disk I/O operations, so
     * normally the copier thread pauses between copy operations to avoid
     * saturating the disk. Once all pages have been copied, the fast copying
     * flag is automatically turned off.
     * 
     * @param fast
     *            <code>true</code> to copy pages at maximum speed.
     * @throws RemoteException
     */
    public void setJournalCopyingFast(boolean fast) throws RemoteException {
        _persistit.getJournalManager().setCopyingFast(fast);
    }

    /**
     * Attempts to close Persistit by invoking {@link Persistit#close}.
     * 
     * @return <code>true</code> if the attempt to close Persistit was
     *         successful; otherwise <code>false</code>
     * @throws RemoteException
     */
    public boolean close() throws RemoteException {
        try {
            _persistit.close();
            return true;
        } catch (PersistitException e) {
            throw new WrappedRemoteException(e);
        }
    }

    /**
     * Attempts to flush and sync all dirty data in Persistit by invoking
     * {@link Persistit#flush} and {@link Persistit#sync}.
     * 
     * @return <code>true</code> if the attempt to close Persistit was
     *         successful; otherwise <code>false</code>
     * @throws RemoteException
     */
    public boolean flushAndSync() throws RemoteException {
        try {
            boolean okay = _persistit.flush();
            if (okay)
                _persistit.sync();
            return okay;
        } catch (PersistitException e) {
            throw new WrappedRemoteException(e);
        }
    }

    /**
     * Returns an array containing a <code>BufferPoolInfo</code> element for
     * each buffer pool. If Persistit is not initialized then this method
     * returns an empty array.
     * 
     * @return The array
     */
    /**
     * Returns an array containing a <code>BufferPoolInfo</code> element for
     * each buffer pool. If Persistit is not initialized then this method
     * returns an empty array.
     * 
     * @return The array
     */
    public BufferPoolInfo[] getBufferPoolInfoArray() {
        HashMap<Integer, BufferPool> bufferPoolTable = _persistit
                .getBufferPoolHashMap();
        int size = bufferPoolTable.size();
        BufferPoolInfo[] result = new BufferPoolInfo[size];
        int index = 0;
        for (int bufferSize = Buffer.MIN_BUFFER_SIZE; bufferSize <= Buffer.MAX_BUFFER_SIZE; bufferSize *= 2) {
            BufferPool pool = (BufferPool) bufferPoolTable.get(new Integer(
                    bufferSize));

            if (pool != null && index < size) {
                BufferPoolInfo info = new BufferPoolInfo();
                pool.populateBufferPoolInfo(info);
                result[index++] = info;
            }
        }
        return result;
    }

    public JournalInfo getJournalInfo() {
        final JournalInfo info = new JournalInfo();
        _persistit.getJournalManager().populateJournalInfo(info);
        return info;
    }

    public RecoveryInfo getRecoveryInfo() {
        final RecoveryInfo info = new RecoveryInfo();
        _persistit.getRecoveryManager().populateRecoveryInfo(info);
        return info;
    }

    public TransactionInfo getTransactionInfo() {
        TransactionInfo info = _transactionInfoCache;
        if (System.currentTimeMillis() - info.getAcquisitionTime() > MAX_STALE) {
            final List<Transaction> transactions = new ArrayList<Transaction>();
            synchronized (info) {
                info.commitCount = 0;
                info.rollbackCount = 0;
                info.rollbackSinceCommitCount = 0;
                _persistit.populateTransactionList(transactions, -1, -1);
                for (final Transaction txn : transactions) {
                    info.commitCount += txn.getCommittedTransactionCount();
                    info.rollbackCount += txn.getRolledBackTransactionCount();
                    info.rollbackSinceCommitCount += txn
                            .getRolledBackSinceLastCommitCount();
                }
                info.updateAcquisitonTime();
            }
        }
        return info;
    }

    public LogicalRecord[] getLogicalRecordArray(String volumeName,
            String treeName, String keyFilterString, KeyState fromKey,
            Key.Direction direction, int maxCount, int maxValueBytes,
            boolean decodeStrings) throws RemoteException {
        LogicalRecord[] records = new LogicalRecord[maxCount];
        int count = 0;
        boolean forward = direction == Key.GT || direction == Key.GTEQ;
        Exchange exchange = null;
        try {
            exchange = _persistit.getExchange(volumeName, treeName, false);
            KeyFilter filter = null;
            if (keyFilterString != null && keyFilterString.length() > 0) {
                filter = new KeyFilter(keyFilterString);
            }
            fromKey.copyTo(exchange.getKey());
            for (; count < maxCount; count++) {
                if (!exchange.traverse(direction, filter, maxValueBytes)) {
                    break;
                } else {
                    LogicalRecord record = new LogicalRecord();
                    record._key = new KeyState(exchange.getKey());
                    record._value = new ValueState(exchange.getValue(),
                            maxValueBytes);

                    if (decodeStrings) {
                        record._keyString = _displayFilter
                                .toKeyDisplayString(exchange);
                        record._valueString = _displayFilter
                                .toValueDisplayString(exchange);
                    }

                    if (forward) {
                        records[count] = record;
                    } else {
                        records[maxCount - count - 1] = record;
                    }
                }
            }
        } catch (Exception e) {
            throw new WrappedRemoteException(e);
        } finally {
            if (exchange != null)
                _persistit.releaseExchange(exchange);
        }
        if (count < maxCount) {
            LogicalRecord[] trimmed = new LogicalRecord[count];
            System.arraycopy(records, forward ? 0 : maxCount - count, trimmed,
                    0, count);
            records = trimmed;
        }
        return records;
    }

    public LogicalRecordCount getLogicalRecordCount(String volumeName,
            String treeName, String keyFilterString, KeyState fromKey,
            Key.Direction direction, int maxCount) throws RemoteException {
        int count = 0;
        Exchange exchange = null;
        KeyState endKeyState = null;

        try {
            exchange = _persistit.getExchange(volumeName, treeName, false);
            exchange.getAuxiliaryKey2().clear();
            KeyFilter filter = null;
            if (keyFilterString != null && keyFilterString.length() > 0) {
                filter = new KeyFilter(keyFilterString);
            }
            fromKey.copyTo(exchange.getKey());
            for (; count < maxCount; count++) {
                if (!exchange.traverse(direction, filter, 0)) {
                    break;
                } else {
                    exchange.getKey().copyTo(exchange.getAuxiliaryKey2());
                }
            }
            endKeyState = new KeyState(exchange.getAuxiliaryKey2());
        } catch (Exception pe) {
            throw new WrappedRemoteException(pe);
        } finally {
            if (exchange != null)
                _persistit.releaseExchange(exchange);
        }
        return new LogicalRecordCount(endKeyState, count);
    }

    /**
     * Returns an array containing a <code>RecordInfo</code> element for each
     * record in the page specified by <code>volumeName</code> and
     * <code>pageAddress</code>. If Persistit is not initialized, or if there is
     * no unique <code>Volume</code> for the specified <code>volumeName</code>,
     * or if there is no page associated with the specified
     * <code>pageAddress</code> or if there is any transient condition that
     * causes the attempt to retrieve records to fail, then this method returns
     * an empty array.
     * 
     * @param volumeName
     * 
     * @param pageAddress
     * 
     * @return the array
     */
    public RecordInfo[] getRecordInfoArray(String volumeName, long pageAddress)
            throws RemoteException {
        Volume volume = _persistit.getVolume(volumeName);
        if (volume == null)
            return new RecordInfo[0];
        try {
            Buffer buffer = volume.getPool().getBufferCopy(volume, pageAddress);
            return buffer.getRecords();
        } catch (PersistitException pe) {
            throw new WrappedRemoteException(pe);
        }
    }

    /**
     * <p>
     * Returns an array of {@link BufferInfo} objects reflecting the states of
     * selected buffers from the <code>BufferPool</code> for the specified
     * <code>bufferSize</code>. The selection criteria include the
     * <code>traversalType</code>, <code>includeMask</code> and
     * <code>excludeMask</code>. See {@link #populateBufferInfoArray} for a
     * similar method that reuses a previously obtained result array.
     * </p>
     * <p>
     * The <code>traversalType</code> must be one of the following:
     * <dl>
     * <dt>0</dt>
     * <dd>all buffers in the buffer pool, in order by <code>poolIndex</code>.</dd>
     * <dt>1</dt>
     * <dd>buffers on the least-recently-used queue, ordered from least- to
     * most-recently used.</dd>
     * <dt>2</dt>
     * <dd>Buffers on the invalid buffer queue. These buffers will be consumed
     * first whenever a new page is copied into the pool.</dd>
     * </dl>
     * </p>
     * <p>
     * The <code>includeMask</code> and <code>excludeMask</code> are applied to
     * each buffer's state to determine whether that buffer should be included
     * in the set returned by this method. If <code>includeMask</code> is
     * <code>null</code> then all buffers are included. Otherwise, only those
     * buffers whose state is selected by <code>includeMask</code> and is not
     * selected by <code>excludeMask</code> are included. Mask values are
     * Strings in which each character denotes an attribute of a
     * <code>Buffer</code> to be included or excluded from the selection. These
     * characters are as follows:
     * <dl>
     * <dt>v</dt>
     * <dd>Buffer must be VALID</dd>
     * <dt>d</dt>
     * <dd>Buffer must be DIRTY</dd>
     * <dt>w</dt>
     * <dd>Buffer must have a WRITER claim</dd>
     * <dt>r</dt>
     * <dd>Buffer must have a READER claim</dd>
     * <dt>p</dt>
     * <dd>Buffer must be PERMANENT. The head page for each {@link Volume}
     * occupies a PERMANENT buffer.</dd> </dd>
     * </p>
     * <p>
     * If Persistit is not initialized then this method returns an empty array.
     * </p>
     * 
     * @param bufferSize
     *            the buffer size of interest
     * 
     * @param traversalType
     *            the traversal type, described above
     * 
     * @param includeMask
     *            the buffer selection include mask, described above
     * 
     * @param excludeMask
     *            the buffer selection exclude mask, described above
     * 
     * @return the array
     */
    public BufferInfo[] getBufferInfoArray(int bufferSize, int traversalType,
            String includeMask, String excludeMask) {
        BufferPool pool = _persistit.getBufferPool(bufferSize);
        if (pool == null)
            return new BufferInfo[0];
        BufferInfo[] results = new BufferInfo[pool.getBufferCount()];
        int count = pool.populateInfo(results, traversalType,
                makeStatus(includeMask), makeStatus(excludeMask));

        if (count < results.length) {
            BufferInfo[] temp = new BufferInfo[count];
            System.arraycopy(results, 0, temp, 0, count);
            results = temp;
        }
        return results;
    }

    /**
     * Returns a <code>BufferInfo</code> reflecting the status of the buffer
     * containing the page specified by the supplied <code>volumeName</code> and
     * <code>pageAddress</code>. If Persisit is not initialized or of the
     * attempt the find the specified page fails, this method returns
     * <code>null</code>
     * 
     * @param volumeName
     *            the name of the volume
     * 
     * @param pageAddress
     *            the page address
     * 
     * @return the BufferInfo for the buffer containing the designated page, of
     *         <code>null</code> if there is none.
     */
    public BufferInfo getBufferInfo(String volumeName, long pageAddress)
            throws RemoteException {
        Volume volume = _persistit.getVolume(volumeName);
        if (volume == null)
            return null;
        try {
            Buffer buffer = volume.getPool().getBufferCopy(volume, pageAddress);
            BufferInfo info = new BufferInfo();
            buffer.populateInfo(info);
            return info;
        } catch (PersistitException pe) {
            throw new WrappedRemoteException(pe);
        }
    }

    /**
     * <p>
     * Populates a supplied array of {@link BufferInfo} objects to reflect the
     * current states of selected buffers from the <code>BufferPool</code> for
     * the specified <code>bufferSize</code>. The selection criteria include the
     * <code>traversalType</code>, <code>includeMask</code> and
     * <code>excludeMask</code>. See {@link #getBufferInfoArray} for a similar
     * method that simply returns a fresh array on each invocation. This method
     * is available for management applications that need to perform frequently
     * refreshes.
     * </p>
     * <p>
     * This method returns the actual number of buffers selected by the supplied
     * criteria. This number may be larger than the size of the supplied array;
     * in this case, information about the first N buffers in the set is
     * returned in the array, where N is the size of the array. An application
     * can use the {@link BufferPoolInfo#getBufferCount} method to determine the
     * maximum number of <code>BufferInfo</code> objects that could be
     * populated.
     * </p>
     * <p>
     * The <code>traversalType</code> must be one of the following:
     * <dl>
     * <dt>0</dt>
     * <dd>all buffers in the buffer pool, in order by <code>poolIndex</code>.</dd>
     * <dt>1</dt>
     * <dd>buffers on the least-recently-used queue, ordered from least- to
     * most-recently used.</dd>
     * <dt>2</dt>
     * <dd>Buffers on the invalid buffer queue. These buffers will be consumed
     * first whenever a new page is copied into the pool.</dd>
     * </dl>
     * </p>
     * <p>
     * The <code>includeMask</code> and <code>excludeMask</code> are applied to
     * each buffer's state to determine whether that buffer should be included
     * in the set returned by this method. If <code>includeMask</code> is
     * <code>null</code> then all buffers are included. Otherwise, only those
     * buffers whose state is selected by <code>includeMask</code> and is not
     * selected by <code>excludeMask</code> are included. Mask values are
     * Strings in which each character denotes an attribute of a
     * <code>Buffer</code> to be included or excluded from the selection. These
     * characters are as follows:
     * <dl>
     * <dt>v</dt>
     * <dd>Buffer must be VALID</dd>
     * <dt>d</dt>
     * <dd>Buffer must be DIRTY</dd>
     * <dt>w</dt>
     * <dd>Buffer must have a WRITER claim</dd>
     * <dt>r</dt>
     * <dd>Buffer must have a READER claim</dd>
     * <dt>p</dt>
     * <dd>Buffer must be PERMANENT. The head page for each {@link Volume}
     * occupies a PERMANENT buffer.</dd>
     * </dd>
     * <p>
     * If Persistit is not initialized then this method returns an empty array.
     * </p>
     * 
     * @param bufferSize
     *            the buffer size of interest
     * 
     * @param traversalType
     *            the traversal type, described above
     * 
     * @param includeMask
     *            the buffer selection include mask, described above
     * 
     * @param excludeMask
     *            the buffer selection exclude mask, described above
     * 
     * @return the array
     */
    public int populateBufferInfoArray(BufferInfo[] results, int bufferSize,
            int traversalType, String includeMask, String excludeMask) {
        BufferPool pool = _persistit.getBufferPool(bufferSize);
        if (pool == null)
            return -1;
        int count = pool.populateInfo(results, traversalType,
                makeStatus(includeMask), makeStatus(excludeMask));
        return count;
    }

    private static int makeStatus(String statusCode) {
        if (statusCode == null)
            return 0;
        int status = 0;
        if (statusCode.indexOf('v') >= 0)
            status |= SharedResource.VALID_MASK;
        if (statusCode.indexOf('d') >= 0)
            status |= SharedResource.DIRTY_MASK;
        if (statusCode.indexOf('r') >= 0)
            status |= SharedResource.CLAIMED_MASK;
        if (statusCode.indexOf('w') >= 0)
            status |= SharedResource.WRITER_MASK;
        if (statusCode.indexOf('s') >= 0)
            status |= SharedResource.SUSPENDED_MASK;
        if (statusCode.indexOf('c') >= 0)
            status |= SharedResource.CLOSING_MASK;
        if (statusCode.indexOf('p') >= 0)
            status |= SharedResource.FIXED_MASK;

        if (statusCode.indexOf('a') >= 0)
            status |= (SharedResource.FIXED_MASK | SharedResource.CLOSING_MASK
                    | SharedResource.SUSPENDED_MASK
                    | SharedResource.WRITER_MASK | SharedResource.CLAIMED_MASK
                    | SharedResource.DIRTY_MASK | SharedResource.VALID_MASK);

        // select none
        if (status == 0)
            status = 0x80000000;

        return status;
    }

    /**
     * Returns an array containing a <code>VolumeInfo</code> element for each
     * open volume. If Persistit is not initialized then this method returns an
     * empty array. </p>
     * 
     * @return The array
     */
    public VolumeInfo[] getVolumeInfoArray() {
        Volume[] volumes = _persistit.getVolumes();
        VolumeInfo[] result = new VolumeInfo[volumes.length];
        for (int index = 0; index < volumes.length; index++) {
            result[index] = new VolumeInfo(volumes[index]);
        }
        return result;
    }

    /**
     * Returns the <code>VolumeInfo</code> for the volume specified by the
     * supplied <code>volumeName</code>. If Persisit is not initialized or there
     * is no unique volume corresponding with the supplied name, then this
     * method returns <code>null</code>.
     * 
     * @param volumeName
     * 
     * @return the <code>VolumeInfo</code>
     */
    public VolumeInfo getVolumeInfo(String volumeName) {
        Volume volume = _persistit.getVolume(volumeName);
        if (volume == null)
            return null;
        else
            return new VolumeInfo(volume);
    }

    /**
     * Returns an array containing a <code>TreeInfo</code> element for each
     * <code>Tree</code> in the specified volume. If there is no volume with the
     * specified name or if Persistit is not initialized then this method
     * returns an empty array.
     * 
     * @param volumeName
     *            The name (or unique partial name) of the volume for which
     *            information is being requested.
     * 
     * @return The array
     */
    public TreeInfo[] getTreeInfoArray(String volumeName)
            throws RemoteException {
        if (volumeName == null)
            return new TreeInfo[0];
        Volume volume = _persistit.getVolume(volumeName);
        if (volume == null)
            return new TreeInfo[0];

        try {
            String[] treeNames = volume.getTreeNames();
            TreeInfo[] results = new TreeInfo[treeNames.length];
            int count = 0;
            for (int index = 0; index < treeNames.length; index++) {
                TreeInfo info = volume.getTreeInfo(treeNames[index]);
                results[count++] = info;
            }
            if (count < results.length) {
                TreeInfo[] temp = new TreeInfo[count];
                System.arraycopy(results, 0, temp, 0, count);
                results = temp;
            }
            return results;
        } catch (PersistitException pe) {
            throw new WrappedRemoteException(pe);
        }
    }

    /**
     * Returns a <code>TreeInfo</code> for a specified <code>Volume</code> and
     * </code>Tree</code>. If Persisit is not initialized, or if no no volume or
     * tree with corresponding names is found, or if there is a transient error
     * in acquiring the information, this method returns <code>null</code>.
     * 
     * @param volumeName
     *            The name (or partial name) of the volume
     * 
     * @param treeName
     *            The name of the tree
     * 
     * @return the <code>TreeInfo</code>
     */
    public TreeInfo getTreeInfo(String volumeName, String treeName)
            throws RemoteException {
        Volume volume = _persistit.getVolume(volumeName);
        if (volume == null)
            return null;
        try {
            Tree tree = null;
            if (Volume.DIRECTORY_TREE_NAME.equals(treeName)) {
                tree = volume.getDirectoryTree();
            } else {
                tree = volume.getTree(treeName, false);
            }
            if (tree != null)
                return new TreeInfo(tree);
        } catch (PersistitException pe) {
            throw new WrappedRemoteException(pe);
        }
        return null;
    }

    /**
     * Returns a Class definition for a class specified by its name. This allows
     * a remote UI instance connected through RMI to load classes that are
     * available within the running Persistit instance so that encoded objects
     * can be inspected within the UI. The implementation of this method should
     * instantiate a new <code>ClassLoader</code> instance so that unreferenced
     * loaded classes may subsequently be garbage collected.
     * 
     * @param className
     *            Fully qualified class name.
     * @return The <code>Class</code>, or <code>null</code> if an exception
     *         occurred while attempting to acquire the Class.
     * @throws RemoteException
     */
    public Class getRemoteClass(String className) throws RemoteException {
        //
        // Need a subclass with a public constructor.
        //
        ClassLoader loader = new ClassLoader() {
        };
        try {
            Class clazz = loader.loadClass(className);
            return clazz;
        } catch (ClassNotFoundException cnfe) {
            throw new WrappedRemoteException(cnfe);
        }
    }

    public int parseKeyFilterString(String keyFilterString)
            throws RemoteException {
        KeyParser parser = new KeyParser(keyFilterString);
        if (parser.parseKeyFilter() != null)
            return -1;
        else
            return parser.getIndex();
    }

/**
     * <p>
     * Decodes the content of the supplied <code>ValueState</code> as an array
     * of Objects. Usually this array has one element containing the single
     * Object value encoded in the <code>ValueState</code>.  However, if multiple
     * items were written to the original <code>Value</code> from which the
     * <code>ValueState</code> was derived in 
     * <a href="com.persistit.Value.html#_streamMode">Stream Mode</a>, this
     * method returns all of the encoded objects.
     * </p>
     * <p>
     * If the <code>valueState</code> represents an undefined value, this method
     * returns an array of length zero.  If the <code>valueState</code> encodes
     * a value of <code>null</code>, then this method returns an array containing
     * one element which is <code>null</code>.
     * </p>
     * 
     * @param valueState    Representation of an encoded {@link Value).
     * 
     * @param context       Object passed to any {@link ValueCoder} used in
     *                      decoding the value. May be <code>null</code>
     * 
     * @return              Array of zero or more Objects encoded
     *                      in the <code>ValueState</code>
     * @throws RemoteException
     */
    public Object[] decodeValueObjects(ValueState valueState,
            CoderContext context) throws RemoteException {
        try {
            Value value = new Value(_persistit);
            valueState.copyTo(value);
            value.setStreamMode(true);
            Vector vector = new Vector();
            while (value.hasMoreItems()) {
                vector.addElement(value.get(null, context));
            }
            Object[] result = new Object[vector.size()];
            for (int index = 0; index < result.length; index++) {
                result[index] = vector.get(index);
            }
            return result;
        } catch (Exception e) {
            throw new WrappedRemoteException(e);
        }
    }

    /**
     * Decodes the content of the supplied <code>KeyState</code> as an array of
     * Objects, one object per <a href="com.persisit.Key.html#_keySegments> key
     * segment</a>.
     * 
     * @param keyState
     *            Representation of an encoded {@link Key}.
     * 
     * @param context
     * 
     * @return
     * 
     * @throws RemoteException
     */
    public Object[] decodeKeyObjects(KeyState keyState, CoderContext context)
            throws RemoteException {
        try {
            Key key = new Key(_persistit);
            keyState.copyTo(key);
            int size = key.getDepth();
            Object[] result = new Object[size];
            for (int index = 0; index < size; index++) {
                result[index] = key.decode(null, context);
            }
            return result;
        } catch (Exception e) {
            throw new WrappedRemoteException(e);
        }
    }

    /**
     * Starts a long-running utility task, such as the integrity checker. The
     * supplied className must identify a subclass of {@link com.persistit.Task}
     * . The number and format of the arguments is specific to the utility task.
     * The returned long value is a unique task ID value used in subsequent
     * calls to {@link #queryTaskStatus}.
     * 
     * @param description
     *            Readable description of this task
     * @param owner
     *            Hostname or username of the user who requested this task
     * @param className
     *            Class name of task to run, e.g.,
     *            <code>com.persistit.IntegrityCheck</code>.
     * @param args
     *            Task-specific parameters
     * @param maximumTime
     *            Maximum wall-clock time (in milliseconds) this Task will be
     *            allowed to run, or 0 for unbounded time
     * @param verbosity
     *            Verbosity level, one of {@link Task#LOG_NORMAL} or
     *            {@link Task#LOG_NORMAL}.
     * @return Task identifier Unique ID for the running task
     * @throws RemoteException
     */
    public long startTask(String description, String owner, String className,
            String[] args, long maximumTime, int verbosity)
            throws RemoteException {
        long taskId;
        synchronized (this) {
            taskId = ++_taskIdCounter;
        }

        try {
            Class<?> clazz = Class.forName(className);
            Task task = (Task) (clazz.newInstance());
            task.setPersistit(_persistit);
            task.setup(taskId, description, owner, maximumTime, verbosity);
            task.setupArgs(args);
            _tasks.put(new Long(taskId), task);
            task.start();
            return taskId;
        } catch (Exception ex) {
            throw new WrappedRemoteException(ex);
        }
    }

    /**
     * Queries the current status of one or all tasks. If the specified taskId
     * value is -1, this method returns status information for all currently
     * active tasks.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param details
     *            <code>true</code> to populate each returned
     *            <code>TaskStatus</code> object with all new messages posted by
     *            the task.
     * @param clear
     *            <code>true</code> to clear all received messages from the
     *            task.
     * @param remove
     *            <code>true</code> to remove the task's status if it has
     *            finished, failed or expired.
     * @throws RemoteException
     */
    public TaskStatus[] queryTaskStatus(long taskId, boolean details,
            boolean clear) {
        if (taskId == -1) {
            int size = _tasks.size();
            int index = 0;
            TaskStatus[] result = new TaskStatus[size];
            for (Iterator iterator = _tasks.values().iterator(); iterator
                    .hasNext();) {
                Task task = (Task) iterator.next();
                TaskStatus ts = new TaskStatus();
                task.populateTaskStatus(ts, details, clear);
                result[index++] = ts;

            }
            return result;
        } else {
            Task task = (Task) _tasks.get(new Long(taskId));
            if (task == null) {
                return new TaskStatus[0];
            } else {
                TaskStatus ts = new TaskStatus();
                task.populateTaskStatus(ts, details, clear);
                return new TaskStatus[] { ts };
            }
        }
    }

    /**
     * Suspend or resumes the task(s) identified by <code>taskId</code>. If
     * <code>taskId</code> is -1, all tasks are modified.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param suspend
     *            <code>true</code> to suspend the task, <code>false</code> to
     *            allow it to resume.
     * @throws RemoteException
     */
    public void setTaskSuspended(long taskId, boolean suspend) {
        if (taskId == -1) {
            for (Iterator iterator = _tasks.values().iterator(); iterator
                    .hasNext();) {
                Task task = (Task) iterator.next();
                if (suspend)
                    task.suspend();
                else
                    task.resume();
            }
        } else {
            Task task = (Task) _tasks.get(new Long(taskId));
            if (task != null) {
                if (suspend)
                    task.suspend();
                else
                    task.resume();
            }
        }
    }

    /**
     * Stops and optionally removes a task specified by its id value. If the
     * task is currently running, this method stops it. If <code>remove</code>
     * is <code>true</code> this method also removes the Task from the task
     * list. Otherwise the task remains on the task list in the
     * {@link Task#STATE_ENDED} state.
     * 
     * @param taskId
     *            Task ID for a selected Task.
     * @throws RemoteException
     */
    public void stopTask(long taskId, boolean remove) {
        if (taskId == -1) {
            for (Iterator<Task> iterator = _tasks.values().iterator(); iterator
                    .hasNext();) {
                Task task = (Task) iterator.next();
                task.stop();
            }
            if (remove) {
                _tasks.clear();
            }
        } else {
            Task task = (Task) _tasks.get(new Long(taskId));
            if (task != null) {
                task.stop();
                if (remove)
                    _tasks.remove(new Long(task._taskId));
            }
        }
    }

    /**
     * Removes finished, expired, stopped or failed tasks from the task list. \
     * This method does not affected running or suspended tasks.
     * 
     * @param taskId
     *            Task ID for a selected task, or -1 for all tasks.
     * 
     * @throws RemoteException
     */
    public void removeFinishedTasks(long taskId) {
        if (taskId == -1) {
            for (Iterator<Task> iterator = _tasks.values().iterator(); iterator
                    .hasNext();) {
                Task task = (Task) iterator.next();
                if (Task.isFinalStatus(task._state)) {
                    iterator.remove();
                }
            }
        } else {
            Long key = Long.valueOf(taskId);
            Task task = (Task) _tasks.get(key);
            if (task != null && Task.isFinalStatus(task._state)) {
                _tasks.remove(key);
            }
        }
    }

    public DisplayFilter getDisplayFilter() {
        return _displayFilter;
    }

    public void setDisplayFilter(DisplayFilter displayFilter) {
        _displayFilter = displayFilter;
    }

    void register(String hostName, String portString) {
        // Note: we don't do this because for now we are not downloading
        // any class files. Persistit should not set a security manager that
        // affects the embedding application!
        //
        // Create and install a security manager.
        //
        // if (System.getSecurityManager() == null)
        // {
        // System.setSecurityManager(new RMISecurityManager());
        // }

        try {
            ManagementImpl impl = (ManagementImpl) _persistit.getManagement();
            int port = -1;
            if (portString != null && portString.length() > 0) {
                try {
                    port = Integer.parseInt(portString);
                    if (hostName == null) {
                        InetAddress addr = InetAddress.getLocalHost();
                        try {
                            hostName = addr.getHostName() + ":" + port;
                        } catch (Exception e) {
                            hostName = addr.getHostAddress() + ":" + port;
                        }
                    }
                } catch (NumberFormatException nfe) {
                }
            }
            if (!_localRegistryCreated && port != -1) {

                if (_persistit.getLogBase().isLoggable(LogBase.LOG_RMI_SERVER)) {
                    _persistit.getLogBase().log(LogBase.LOG_RMI_SERVER,
                            "Creating RMI Registry on port " + port);
                }
                LocateRegistry.createRegistry(port);
                _localRegistryCreated = true;
            }

            if (hostName != null && hostName.length() > 0) {

                String name = "//" + hostName + "/PersistitManagementServer";
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_RMI_SERVER)) {
                    _persistit.getLogBase().log(LogBase.LOG_RMI_SERVER,
                            "Registering Management RMI object to name", name);
                }

                UnicastRemoteObject.exportObject(impl);
                Naming.rebind(name, impl);

                impl._registered = true;
                impl._registeredHostName = hostName;

                if (_persistit.getLogBase().isLoggable(LogBase.LOG_RMI_SERVER)) {
                    _persistit.getLogBase().log(LogBase.LOG_RMI_SERVER,
                            "Successfully registered", hostName);
                }
            }
        } catch (Exception exception) {
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_RMI_EXCEPTION)) {
                _persistit.getLogBase().log(LogBase.LOG_RMI_EXCEPTION,
                        hostName, exception);
            }
        }
    }

    void unregister() {
        if (_registered && _persistit.isInitialized()) {
            try {
                ManagementImpl impl = (ManagementImpl) _persistit
                        .getManagement();

                UnicastRemoteObject.unexportObject(impl, true);
                _registered = false;
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_RMI_SERVER)) {
                    _persistit.getLogBase().log(LogBase.LOG_RMI_SERVER,
                            _registeredHostName, "Unregistered");
                }
            } catch (Exception exception) {
                if (_persistit.getLogBase().isLoggable(
                        LogBase.LOG_RMI_EXCEPTION)) {
                    _persistit.getLogBase().log(LogBase.LOG_RMI_EXCEPTION,
                            _registeredHostName, exception);
                }
            }
        }
    }

    public String execute(final String commandLine) {
        try {
            final ManagementCommand command = ManagementCommand
                    .parse(commandLine);
            long taskId;

            Task task = (Task) (command.getTaskClass().newInstance());
            task.setPersistit(_persistit);
            task.setupTaskWithArgParser(command.getArgs());
            if (task.isImmediate()) {
                task.runTask();
                return task.getStatusDetail();
            } else {
                synchronized (this) {
                    taskId = ++_taskIdCounter;
                }
                task.setup(taskId, commandLine, "cli", 0, 5);
                _tasks.put(new Long(taskId), task);
                task.start();
                return taskId + ": started";
            }

        } catch (Exception ex) {
            return "Failed: " + ex.toString();
        }
    }
}