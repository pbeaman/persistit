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
 * Created on Aug 27, 2004
 */
package com.persistit;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;

import com.persistit.encoding.CoderContext;

/**
 * <p>
 * Interface for a service object that exposes information about the Persistit
 * environment. With this public API, embedding applications can query
 * performance metrics and resources within Persistit that are not exposed by
 * the normal access methods. For example, this class provides methods that
 * enumerate the volumes currently mounted, the size and effectiveness of buffer
 * pool components, and many other items that may be useful in tuning and
 * managing Persistit.
 * 
 * @version 1.0
 *          </p>
 */
public interface Management extends Remote {
    /**
     * Indicates whether Persistit is currently in the initialized state.
     * 
     * @return The state
     */
    public boolean isInitialized() throws RemoteException;

    /**
     * Returns the version name of the current Peristit instance.
     * 
     * @return the version name
     */
    public String getVersion() throws RemoteException;

    /**
     * Returns the copyright notice for the current Persistit instance.
     * 
     * @return the copyright notice
     */
    public String getCopyright() throws RemoteException;

    /**
     * Returns the system time at which Persistit was initialized.
     * 
     * @return start time, in milliseconds since January 1, 1970 00:00:00 GMT.
     */
    public long getStartTime() throws RemoteException;

    /**
     * Returns the elapsed time since startup in milliseconds
     * 
     * @return elapsed time in milliseconds
     */
    public long getElapsedTime() throws RemoteException;

    /**
     * Indicates whether Persistit will suspend its shutdown activities on
     * invocation of {@link com.persistit.Persistit#close}. This flag is
     * intended for use by management tools that need to keep Persistit open
     * even when the application has requested it to close so that the final
     * state of the Persistit environment can be examined.
     * 
     * @return <tt>true</tt> if Persistit will wait when attempting to close;
     *         <tt>false</tt> if the <tt>close</tt> operation will not be
     *         suspended.
     */
    public boolean isShutdownSuspended() throws RemoteException;

    /**
     * Controls whether Persistit will suspend its shutdown activities on
     * invocation of {@link com.persistit.Persistit#close}. This flag is
     * intended for use by management tools that need to keep Persistit open
     * even when the application has requested it to close so that the final
     * state of the Persistit environment can be examined.
     * 
     * @param suspended
     *            <tt>true</tt> to specify that Persistit will wait when
     *            attempting to close; otherwise <tt>false</tt>.
     */
    public void setShutdownSuspended(boolean suspended) throws RemoteException;

    /**
     * Controls whether Persistit will suspend the thread that copies pages
     * from the journal back to their respective Volumes. This flag is used
     * by tools that provide on-line backup.
     * 
     * @param suspended
     *            <tt>true</tt> to specify that Persistit will suspend
     *            journal copying; otherwise <tt>false</tt>.
     */
    public void setJournalCopyingSuspended(boolean suspended) throws RemoteException;

    /**
     * Indicates whether Persistit is suspending all update operations. When
     * enabled, Persistit will indefinitely delay each Thread that attempts to
     * perform an update operation.
     * 
     * @return <tt>true</tt> if Persistit will suspend any attempt to update a
     *         <tt>Volume</tt>; otherwise <tt>false</tt>.
     * @throws RemoteException
     */
    public boolean isUpdateSuspended() throws RemoteException;

    /**
     * Controls whether Persistit will suspend all update operations. When
     * enabled, Persistit will delay each Thread that attempts to perform an
     * update operation indefinitely.
     * 
     * @param suspended
     * @throws RemoteException
     */
    public void setUpdateSuspended(boolean suspended) throws RemoteException;

    /**
     * Attempts to close Persistit by invoking {@link Persistit#close}.
     * 
     * @return <tt>true</tt> if the attempt to close Persistit was successful;
     *         otherwise <tt>false</tt>
     * @throws RemoteException
     */
    public boolean close() throws RemoteException;

    /**
     * Attempts to flush and sync all dirty data in Persistit by invoking
     * {@link Persistit#flush} and {@link Persistit#sync}.
     * 
     * @return <tt>true</tt> if the attempt to close Persistit was successful;
     *         otherwise <tt>false</tt>
     * @throws RemoteException
     */
    public boolean flushAndSync() throws RemoteException;

    /**
     * Returns an array containing a <tt>BufferPoolInfo</tt> element for each
     * buffer pool. If Persistit is not initialized then this method returns an
     * empty array.
     * 
     * @return The array
     */
    public BufferPoolInfo[] getBufferPoolInfoArray() throws RemoteException;

    /**
     * Returns an array containing a <tt>RecordInfo</tt> element for each record
     * in the page specified by <tt>volumeName</tt> and <tt>pageAddress</tt>. If
     * Persistit is not initialized, or if there is no unique <tt>Volume</tt>
     * for the specified <tt>volumeName</tt>, or if there is no page associated
     * with the specified <tt>pageAddress</tt> or if there is any transient
     * condition that causes the attempt to retrieve records to fail, then this
     * method returns an empty array.
     * 
     * @param volumeName
     *            The volume name
     * 
     * @param pageAddress
     *            The page addres
     * 
     * @return the array Array of <tt>RecordInfo</tt> objects containing
     *         detailed information about the records on the specified page.
     */
    public RecordInfo[] getRecordInfoArray(String volumeName, long pageAddress)
            throws RemoteException;

    /**
     * <p>
     * Returns an array of <tt>LogicalRecord</tt> elements from a <tt>Tree</tt>.
     * The tree is specified by the provided <tt>volumeName</tt> and
     * <tt>treeName</tt> values. The size of the the returned array is bounded
     * by <tt>maxRecordCount</tt>. Records whose <tt>Value</tt> fields exceed
     * <tt>maxValueBytes</tt> in length are truncated to that size. This allows
     * a client program to limit the maximum number of bytes to be transmitted
     * in one RMI request. If non-<tt>null</tt>, <tt>keyFilterString</tt>
     * specifies a <tt>KeyFilter</tt> to be applied when taversing records. The
     * supplied <tt>fromKey</tt> specifies a starting key value, and is non-
     * inclusive.
     * </p>
     * <p>
     * Each <tt>LogicalRecord</tt> object returned by this method optionally
     * contain decoded String representations of the record's key and value. If
     * <tt>decodeString</tt> is <tt>true</tt> then each <tt>LogicalRecord</tt>'s
     * <tt>KeyState</tt>, <tt>KeyString</tt> and <tt>ValueString</tt> properties
     * are set to String values resulting from decoding the underlying key and
     * value in the context of the running Persistit instance. Otherwise,
     * <tt>KeyString</tt> and <tt>ValueString</tt> are <tt>null</tt>.
     * Applications that display String-valued representations of a record's key
     * and value should typically enable <tt>decodeString</tt> so that any
     * required {@link com.persistit.encoding.KeyCoder} or
     * {@link com.persistit.encoding.ValueCoder} objects are referenced in the
     * context of the running Persistit instance, not an RMI client.
     * </p>
     * 
     * @param volumeName
     *            The volume name
     * 
     * @param treeName
     *            The tree name
     * 
     * @param keyFilterString
     *            If non-<tt>null</tt>, specifies the String representation of a
     *            {@link KeyFilter}. Only records having key values selected by
     *            this filter are returned.
     * 
     * @param fromKey
     *            Starting key value.
     * 
     * @param direction
     *            Traversal direction.
     * 
     * @param maxRecordCount
     *            Maximum number of LogicalRecord objects to return
     * 
     * @param maxValueBytes
     *            Maximum encoded size of each Value object.
     * 
     * @param decodeStrings
     *            If <tt>true</tt>, decode each <tt>LogicalRecord</tt>'s
     *            <tt>Key</tt> and <tt>Value</tt> as Strings.
     * 
     * @return Array of LogicalRecord objects
     * 
     * @throws RemoteException
     */
    public LogicalRecord[] getLogicalRecordArray(String volumeName,
            String treeName, String keyFilterString, KeyState fromKey,
            Key.Direction direction, int maxRecordCount, int maxValueBytes,
            boolean decodeStrings) throws RemoteException;

    /**
     * Returns a {@link JournalInfo} structure describing the current state if
     * the Journal.
     * 
     * @return the JournalInfo
     * @throws RemoteException
     */
    public JournalInfo getJournalInfo() throws RemoteException;

    /**
     * <p>
     * Counts the the number of records that could be traversed given a starting
     * key value in a tree specified by <tt>volumeName</tt>, <tt>treeName</tt>,
     * using an optional {@link KeyFilter} specified by <tt>keyFilterString</tt>
     * . Records are counted by traversing forward or backward according to the
     * Persistit <a href="Key.html#_keyOrdering">key order specification</a>.
     * The direction of traversal is specified by <tt>direction</tt>.
     * </p>
     * <p>
     * The returned <tt>LogicalRecordCount</tt> contains the count of records
     * actually traversed by this method, which will never exceed
     * <tt>maximumCount</tt>. It also contains a <tt>KeyState</tt> representing
     * the value of the <tt>Key</tt> of the final record traversed by this
     * method. If the returned count is N then the returned <tt>KeyState</tt>
     * corresponds with the Nth record counted.
     * </p>
     * 
     * @param volumeName
     *            The volume name
     * 
     * @param treeName
     *            The tree name
     * 
     * @param keyFilterString
     *            If non-<tt>null</tt>, specifies the String representation of a
     *            {@link KeyFilter}. Only records having key values selected by
     *            this filter are returned.
     * 
     * @param fromKey
     *            Starting key value.
     * 
     * @param direction
     *            Traversal direction.
     * 
     * @param maximumCount
     *            Maximum number of LogicalRecord objects to traverse.
     * 
     * @return The traversed count, and a value of the key associated with the
     *         last record traversed.
     * 
     * @throws RemoteException
     */

    public LogicalRecordCount getLogicalRecordCount(String volumeName,
            String treeName, String keyFilterString, KeyState fromKey,
            Key.Direction direction, int maximumCount) throws RemoteException;

    /**
     * <p>
     * Returns an array of {@link BufferInfo} objects reflecting the states of
     * selected buffers from the <tt>BufferPool</tt> for the specified
     * <tt>bufferSize</tt>. The selection criteria include the
     * <tt>traversalType</tt>, <tt>includeMask</tt> and <tt>excludeMask</tt>.
     * See {@link #populateBufferInfoArray} for a similar method that reuses a
     * previously obtained result array.
     * </p>
     * <p>
     * The <tt>traversalType</tt> must be one of the following:
     * <dl>
     * <dt>0</dt>
     * <dd>all buffers in the buffer pool, in order by <tt>poolIndex</tt>.</dd>
     * <dt>1</dt>
     * <dd>buffers on the least-recently-used queue, ordered from least- to
     * most-recently used.</dd>
     * <dt>2</dt>
     * <dd>Buffers on the invalid buffer queue. These buffers will be consumed
     * first whenever a new page is copied into the pool.</dd>
     * </dl>
     * </p>
     * <p>
     * The <tt>includeMask</tt> and <tt>excludeMask</tt> are applied to each
     * buffer's state to determine whether that buffer should be included in the
     * set returned by this method. If <tt>includeMask</tt> is <tt>null</tt>
     * then all buffers are included. Otherwise, only those buffers whose state
     * is selected by <tt>includeMask</tt> and is not selected by
     * <tt>excludeMask</tt> are included. Mask values are Strings in which each
     * character denotes an attribute of a <tt>Buffer</tt> to be included or
     * excluded from the selection. These characters are as follows:
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
            String includeMask, String excludeMask) throws RemoteException;

    /**
     * Returns a <tt>BufferInfo</tt> reflecting the status of the buffer
     * containing the page specified by the supplied <tt>volumeName</tt> and
     * <tt>pageAddress</tt>. If Persisit is not initialized or of the attempt
     * the find the specified page fails, this method returns <tt>null</tt>
     * 
     * @param volumeName
     *            the name of the volume
     * 
     * @param pageAddress
     *            the page address
     * 
     * @return the BufferInfo for the buffer containing the designated page, of
     *         <tt>null</tt> if there is none.
     */
    public BufferInfo getBufferInfo(String volumeName, long pageAddress)
            throws RemoteException;

    /**
     * <p>
     * Populates a supplied array of {@link BufferInfo} objects to reflect the
     * current states of selected buffers from the <tt>BufferPool</tt> for the
     * specified <tt>bufferSize</tt>. The selection criteria include the
     * <tt>traversalType</tt>, <tt>includeMask</tt> and <tt>excludeMask</tt>.
     * See {@link #getBufferInfoArray} for a similar method that simply returns
     * a fresh array on each invocation. This method is available for management
     * applications that need to perform frequently refreshes.
     * </p>
     * <p>
     * This method returns the actual number of buffers selected by the supplied
     * criteria. This number may be larger than the size of the supplied array;
     * in this case, information about the first N buffers in the set is
     * returned in the array, where N is the size of the array. An application
     * can use the {@link BufferPoolInfo#getBufferCount} method to determine the
     * maximum number of <tt>BufferInfo</tt> objects that could be populated.
     * </p>
     * <p>
     * The <tt>traversalType</tt> must be one of the following:
     * <dl>
     * <dt>0</dt>
     * <dd>all buffers in the buffer pool, in order by <tt>poolIndex</tt>.</dd>
     * <dt>1</dt>
     * <dd>buffers on the least-recently-used queue, ordered from least- to
     * most-recently used.</dd>
     * <dt>2</dt>
     * <dd>Buffers on the invalid buffer queue. These buffers will be consumed
     * first whenever a new page is copied into the pool.</dd>
     * </dl>
     * </p>
     * <p>
     * The <tt>includeMask</tt> and <tt>excludeMask</tt> are applied to each
     * buffer's state to determine whether that buffer should be included in the
     * set returned by this method. If <tt>includeMask</tt> is <tt>null</tt>
     * then all buffers are included. Otherwise, only those buffers whose state
     * is selected by <tt>includeMask</tt> and is not selected by
     * <tt>excludeMask</tt> are included. Mask values are Strings in which each
     * character denotes an attribute of a <tt>Buffer</tt> to be included or
     * excluded from the selection. These characters are as follows:
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
            int traversalType, String includeMask, String excludeMask)
            throws RemoteException;

    /**
     * Returns an array containing a <tt>VolumeInfo</tt> element for each open
     * volume. If Persistit is not initialized then this method returns an empty
     * array. </p>
     * 
     * @return The array
     */
    public VolumeInfo[] getVolumeInfoArray() throws RemoteException;

    /**
     * Returns a Class definition for a class specified by its name. This allows
     * a remote UI instance connected through RMI to load classes that are
     * available within the running Persistit instance so that encoded objects
     * can be inspected within the UI. The implementation of this method should
     * instantiate a new <tt>ClassLoader</tt> instance so that unreferenced
     * loaded classes may subsequently be garbage collected.
     * 
     * @param className
     *            Fully qualified class name.
     * @return The <tt>Class</tt>, or <tt>null</tt> if an exception occurred
     *         while attempting to acquire the Class.
     * @throws RemoteException
     */
    public Class getRemoteClass(String className) throws RemoteException;

    /**
     * Returns the <tt>VolumeInfo</tt> for the volume specified by the supplied
     * <tt>volumeName</tt>. If Persisit is not initialized or there is no unique
     * volume corresponding with the supplied name, then this method returns
     * <tt>null</tt>.
     * 
     * @param volumeName
     * 
     * @return the <tt>VolumeInfo</tt>
     */
    public VolumeInfo getVolumeInfo(String volumeName) throws RemoteException;

    /**
     * Returns an array containing a <tt>TreeInfo</tt> element for each
     * <tt>Tree</tt> in the specified volume. If there is no volume with the
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
            throws RemoteException;

    /**
     * Returns a <tt>TreeInfo</tt> for a specified <tt>Volume</tt> and
     * </tt>Tree</tt>. If Persisit is not initialized, or if no no volume or
     * tree with corresponding names is found, or if there is a transient error
     * in acquiring the information, this method returns <tt>null</tt>.
     * 
     * @param volumeName
     *            The name (or partial name) of the volume
     * 
     * @param treeName
     *            The name of the tree
     * 
     * @return the <tt>TreeInfo</tt>
     */
    public TreeInfo getTreeInfo(String volumeName, String treeName)
            throws RemoteException;

    /**
     * Parses the supply String to determine whether it is a valid
     * <tt>KeyFilter</tt>. Returns the index of the first incorrect character in
     * the supplied String, or -1 if the string is a valid String representation
     * of a KeyFilter.
     * 
     * @param keyFilterString
     * 
     * @return index of first invalid character in the supplied
     *         <tt>keyFilterString</tt>, or -1 if the string is valid.
     * 
     * @throws RemoteException
     */
    public int parseKeyFilterString(String keyFilterString)
            throws RemoteException;

    /**
     * <p>
     * Decodes the content of the supplied <tt>ValueState</tt> as an array of
     * Objects. Usually this array has one element containing the single object
     * value encoded in the <tt>ValueState</tt>. However, if multiple items were
     * written to the original <tt>Value</tt> from which the <tt>ValueState</tt>
     * was derived in <a href="Value.html#_streamMode">Stream Mode</a>, this
     * method returns all of the encoded objects.
     * </p>
     * <p>
     * If the <tt>valueState</tt> represents an undefined value, this method
     * returns an array of length zero. If the <tt>valueState</tt> encodes a
     * value of <tt>null</tt>, then this method returns an array containing one
     * element which is <tt>null</tt>.
     * </p>
     * 
     * @param valueState
     *            Representation of an encoded {@link Value}.
     * 
     * @param context
     *            Object passed to any {@link com.persistit.encoding.ValueCoder}
     *            used in decoding the value. May be <tt>null</tt>.
     * 
     * @return Array of zero or more decoded objects
     * @throws RemoteException
     */
    public Object[] decodeValueObjects(ValueState valueState,
            CoderContext context) throws RemoteException;

    /**
     * Decodes the content of the supplied <tt>KeyState</tt> as an array of
     * Objects, one object per <a href="Key.html#_keySegments"> key segment</a>.
     * 
     * @param keyState
     *            Representation of an encoded {@link Key}.
     * 
     * @param context
     *            Object passed to any {@link com.persistit.encoding.KeyCoder}
     *            used in decoding the value. May be <tt>null</tt>
     * 
     * @return Array of decoded key segments
     * 
     * @throws RemoteException
     */
    public Object[] decodeKeyObjects(KeyState keyState, CoderContext context)
            throws RemoteException;

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
     *            <tt>com.persistit.IntegrityCheck</tt>.
     * @param args
     *            Task-specific parameters
     * @param maximumTime
     *            Maximum wall-clock time (in milliseconds) this Task will be
     *            allowed to run
     * @param verbosity
     *            Verbosity level, one of {@link Task#LOG_NORMAL} or
     *            {@link Task#LOG_VERBOSE}.
     * @return Task identifier Unique ID for the running task
     * @throws RemoteException
     */
    public long startTask(String description, String owner, String className,
            String[] args, long maximumTime, int verbosity)
            throws RemoteException;

    /**
     * Queries the current status of one or all tasks. If the specified taskId
     * value is -1, this method returns status information for all currently
     * active tasks.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param details
     *            <tt>true</tt> to populate each returned <tt>TaskStatus</tt>
     *            object with all new messages posted by the task.
     * @param clear
     *            <tt>true</tt> to clear all received messages from the task.
     * @return Array of <tt>TaskStatus</tt> objects indicating status of
     *         selected task(s).
     * @throws RemoteException
     */
    public TaskStatus[] queryTaskStatus(long taskId, boolean details,
            boolean clear) throws RemoteException;

    /**
     * Suspend or resume the task(s) identified by <tt>taskId</tt>. If
     * <tt>taskId</tt> is -1, all tasks are modified.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param suspend
     *            <tt>true</tt> to suspend the task, <tt>false</tt> to allow it
     *            to resume.
     * @throws RemoteException
     */
    public void setTaskSuspended(long taskId, boolean suspend)
            throws RemoteException;

    /**
     * Stops and optionally removes a task specified by its id value. If the
     * task is currently running, this method stops it. If <tt>remove</tt> is
     * <tt>true</tt> this method also removes the Task from the task list.
     * Otherwise the task remains on the task list in the
     * {@link Task#STATE_ENDED} state. If <tt>taskId</tt> is -1 then these
     * actions are applied to all tasks.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @throws RemoteException
     */
    public void stopTask(long taskId, boolean remove) throws RemoteException;

    public DisplayFilter getDisplayFilter() throws RemoteException;

    public void setDisplayFilter(DisplayFilter displayFilter)
            throws RemoteException;

    /**
     * A DisplayFilter formats the String value presented in the Tree tab of the
     * AdminUI. Invoke {@link ManagementImpl#setDisplayFilter(DisplayFilter)} to
     * supplyu a non-default DisplayFilter.
     * 
     * @author peter
     * 
     */
    public interface DisplayFilter {

        public String toKeyDisplayString(final Exchange exchange);

        public String toValueDisplayString(final Exchange exchange);

    }

    /**
     * Base class for all management data structures that retain the time at
     * which the information was recorded.
     */
    abstract static class AcquisitionTimeBase implements Serializable {

        private static final long serialVersionUID = 5162380870350766401L;

        long _time;

        private AcquisitionTimeBase() {
            updateAcquisitonTime();
        }

        /**
         * Returns the time (in milliseconds since January 1, 1970 00:00:00 GMT)
         * at which the information represented by this object was acquired.
         * 
         * @return the information acquisition time
         */
        public long getAcquisitionTime() {
            return _time;
        }

        void updateAcquisitonTime() {
            _time = System.currentTimeMillis();
        }
    }

    /**
     * Exposes information about a BufferPool. A BufferPool is a container for
     * all the buffers of a particular size. The {@link #getBufferPoolInfoArray}
     * method returns an array containing BufferPoolInfo elements for each
     * buffer size.
     */
    public static class BufferPoolInfo extends AcquisitionTimeBase implements
            Serializable {
        public final static long serialVersionUID = 9044282840712593435L;

        int _bufferSize;
        int _bufferCount;
        long _getCounter;
        long _hitCounter;
        int _validPageCount;
        int _dirtyPageCount;
        int _readerClaimedPageCount;
        int _writerClaimedPageCount;

        /**
         * Returns the size of <tt>Buffer</tt>s managed by this pool.
         * 
         * @return The size in bytes of each buffer in this pool
         */
        public int getBufferSize() {
            return _bufferSize;
        }

        /**
         * Returns the count of <tt>Buffer</tt>s managed by this pool.
         * 
         * @return The count
         */
        public int getBufferCount() {
            return _bufferCount;
        }

        /**
         * Returns the count of lookup operations for pages images in this pool.
         * This number, in comparison with the hit counter, indicates how
         * effective the cache is in reducing disk I/O.
         * 
         * @return The get count
         */
        public long getGetCounter() {
            return _getCounter;
        }

        /**
         * Returns the count of lookup operations for pages images in this pool
         * for which the page image was already found in this
         * <tt>BufferPool</tt>. This number, in comparison with the get counter,
         * indicates how effective the cache is in reducing disk I/O.
         * 
         * @return The hit count
         */
        public long getHitCounter() {
            return _hitCounter;
        }

        /**
         * Get the "hit ratio" - the number of hits divided by the number of
         * overall gets. A value close to 1.0 indicates that most attempts to
         * find data in the <tt>BufferPool</tt> are successful - i.e., that the
         * cache is effectively reducing the need for disk read operations.
         * 
         * @return The ratio
         */
        public double getHitRatio() {
            if (_getCounter == 0)
                return 0.0;
            else
                return ((double) _hitCounter) / ((double) _getCounter);
        }

        /**
         * Get the count of valid pages in this pool.
         * 
         * @return The count of valid pages in this pool
         */
        public int getValidPageCount() {
            return _validPageCount;
        }

        /**
         * Get the count of dirty pages (pages that contain updates not yet
         * written to disk) in this pool.
         * 
         * @return The count of dirty pages in this pool
         */
        public int getDirtyPageCount() {
            return _dirtyPageCount;
        }

        /**
         * Get the count of pages on which running threads have reader
         * (non-exclusive), but <i>not</i> writer (exclusive) claims in this
         * pool.
         * 
         * @return The count of pages with reader claims
         */
        public int getReaderClaimedPageCount() {
            return _readerClaimedPageCount;
        }

        /**
         * Get the count of pages on which running threads have writer
         * (exclusive) claims in this pool.
         * 
         * @return The count of pages with writer claims
         */
        public int getWriterClaimedPageCount() {
            return _writerClaimedPageCount;
        }
    }

    /**
     * <p>
     * Exposes information about one <tt>Buffer</tt>. Use the
     * {@link #getBufferInfoArray} method to get an array of <tt>BufferInfo</tt>
     * s for <tt>Buffers</tt> in various states.
     * </p>
     * <p>
     * Note that the content of a <tt>BufferPool</tt> and the status of
     * individual <tt>Buffer</tt>s changes extremely rapidly when Persistit is
     * active. A <tt>BufferInfo</tt> represents a snapshot of this state at the
     * acquisition time.
     * </p>
     */
    public static class BufferInfo extends AcquisitionTimeBase implements
            Serializable {
        public final static long serialVersionUID = -7847730868509629017L;

        int _poolIndex;
        int _type;
        String _typeName;
        int _status;
        String _statusCode;
        String _writerThreadName;
        long _pageAddress;
        long _rightSiblingAddress;
        String _volumeName;
        long _volumeId;
        long timestamp;
        int _bufferSize;
        int _availableBytes;
        int _alloc;
        int _slack;
        int _keyBlockStart;
        int _keyBlockEnd;

        /**
         * Indicates whether the page occupying this buffer is a Garbage page.
         * 
         * @return <tt>true</tt> if this is a Garbage page, otherwise
         *         <tt>false</tt>.
         */
        public boolean isGarbagePage() {
            return _type == Buffer.PAGE_TYPE_GARBAGE;
        }

        /**
         * Indicates whether the page occupying this buffer is a Data page.
         * 
         * @return <tt>true</tt> if this is a Data page, otherwise
         *         <tt>false</tt>.
         */
        public boolean isDataPage() {
            return _type == Buffer.PAGE_TYPE_DATA;
        }

        /**
         * Indicates whether the page occupying this buffer is an Index page.
         * 
         * @return <tt>true</tt> if this is an Index page, otherwise
         *         <tt>false</tt>.
         */
        public boolean isIndexPage() {
            return _type >= Buffer.PAGE_TYPE_INDEX_MIN
                    && _type <= Buffer.PAGE_TYPE_INDEX_MAX;
        }

        /**
         * Indicates whether the page occupying this buffer is a LongRecord
         * page.
         * 
         * @return <tt>true</tt> if this is a LongRecord page, otherwise
         *         <tt>false</tt>.
         */
        public boolean isLongRecordPage() {
            return _type == Buffer.PAGE_TYPE_LONG_RECORD;
        }

        /**
         * Indicates whether the page occupying this buffer is an Unallocated
         * page.
         * 
         * @return <tt>true</tt> if this is an Unallocated page, otherwise
         *         <tt>false</tt>.
         */
        public boolean isUnallocatedPage() {
            return _type == Buffer.PAGE_TYPE_UNALLOCATED;
        }

        /**
         * Returns the position of this <tt>Buffer</tt> within the
         * <tt>BufferPool</tt>.
         * 
         * @return The index
         */
        public int getPoolIndex() {
            return _poolIndex;
        }

        /**
         * Returns the type of page occupying this <tt>Buffer</tt> as an
         * integer. See {@link #getTypeName} for a displayable form.
         * 
         * @return The type.
         */
        public int getType() {
            return _type;
        }

        /**
         * Returns the type of page occupying this <tt>Buffer</tt> in
         * displayable name form. Values include
         * 
         * <pre>
         * <code>
         *    Unused
         *    Data
         *    Index<i>NN</i>
         *    Garbage
         *    LongRecord
         * </code>
         * </pre>
         * 
         * where <i>NN</i> indicates index level counting from the leaf data
         * page.
         * 
         * @return The displayable page type
         */
        public String getTypeName() {
            return _typeName;
        }

        /**
         * Returns the status integer for this <tt>Buffer</tt>. See
         * {@link #getStatusName} for a displayable form.
         * 
         * @return The status integer.
         */
        public int getStatus() {
            return _status;
        }

        /**
         * Returns the status of this <tt>Buffer</tt> in displayable form.
         * Character flags in the string value include:
         * 
         * <pre>
         *   <code>v</code> - valid
         *   <code>r</code> - reader claim
         *   <code>w</code> - writer claim
         *   <code>d</code> - dirty
         *   <code><i>nnn</i></code> - count of threads with reader claims
         * </pre>
         * 
         * @return The status in displayable form
         */
        public String getStatusName() {
            return _statusCode;
        }

        /**
         * Returns the name of the <tt>Thread</tt> that currently holds a writer
         * (exclusive) claim on the <tt>Buffer</tt> if there is one, otherwise
         * returns <tt>null</tt>.
         * 
         * @return The thread name, or <tt>null</tt> if there is none
         */
        public String getWriterThreadName() {
            return _writerThreadName;
        }

        /**
         * Returns the page address of the page currently occupying the
         * <tt>Buffer</tt>. The address is an ordinal number that indicates the
         * page's position within a volume file.In a standard Volume, page
         * address <code>P</code> is located at file address
         * <code>P * pageSize</code>. Page address 0 denotes the head page of
         * the Volume.
         * 
         * @return The page address.
         */
        public long getPageAddress() {
            return _pageAddress;
        }

        /**
         * Returns the page address of the next page in key order on this level.
         * 
         * @return The page address of the right sibling page.
         */
        public long getRightSiblingAddress() {
            return _rightSiblingAddress;
        }

        /**
         * Returns the full pathname of the Volume of the page occupying this
         * buffer if there is one, otherwise returns <tt>null</tt>.
         * 
         * @return the Volume Name
         */
        public String getVolumeName() {
            return _volumeName;
        }

        /**
         * Returns the internal ID value of the Volume of the page occupying
         * this buffer if there is one, otherwise returns 0.
         * 
         * @return the volume ID
         */
        public long getVolumeId() {
            return _volumeId;
        }

        /**
         * Returns the count of times the state of the page occupying this
         * buffer has changed.
         * 
         * @return The change count
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * Returns the buffer size, in bytes
         * 
         * @return the size
         */
        public int getBufferSize() {
            return _bufferSize;
        }

        /**
         * Returns the number of unused bytes available to hold additional
         * key/value pairs.
         * 
         * @return The number of avaiable bytes in the <tt>Buffer</tt>.
         */
        public int getAvailableSize() {
            return _availableBytes;
        }

        /**
         * Returns an offset within the <tt>Buffer</tt> used internally in
         * allocating space for key/value pairs.
         * 
         * @return The alloc offset
         */
        public int getAlloc() {
            return _alloc;
        }

        /**
         * Returns a size used internally in allocating space for key/value
         * pairs.
         * 
         * @return The slack size
         */
        public int getSlack() {
            return _slack;
        }

        /**
         * Returns an offset within the <tt>Buffer</tt> used internally in
         * allocating space for key/value pairs.
         * 
         * @return The keyBlockStart offset
         */
        public int getKeyBlockStart() {
            return _keyBlockStart;
        }

        /**
         * Returns an offset within the <tt>Buffer</tt> used internally in
         * allocating space for key/value pairs.
         * 
         * @return The keyBlockEnd offset
         */
        public int getKeyBlockEnd() {
            return _keyBlockEnd;
        }

        /**
         * Returns a human-readable summary of information about the page
         * contained in this <tt>Buffer</tt>.
         * 
         * @return a one-line displayable summary
         */
        public String displayableSummary() {
            return "page=" + _pageAddress + " type=" + _typeName
                    + " rightSibling=" + _rightSiblingAddress + " status="
                    + _statusCode + " start=" + _keyBlockStart + " end="
                    + _keyBlockEnd + " size=" + _bufferSize + " alloc="
                    + _alloc + " slack=" + _slack + " index=" + _poolIndex
                    + " change count=" + timestamp;
        }
    }

    /**
     * Exposes information about a Volume. The {@link #getVolumeInfoArray}
     * returns information about all open Volumes in an array of
     * <tt>VolumeInfo</tt> elements.
     */
    public static class VolumeInfo extends AcquisitionTimeBase implements
            Serializable {
        public final static long serialVersionUID = -1231320633942497896L;

        int _bufferSize;
        String _path;
        String _name;
        long _id;
        long _createTime;
        long _openTime;
        long _generation;
        long _getCounter;
        long _readCounter;
        long _writeCounter;
        long _lastRead;
        long _lastWrite;
        long _lastExtension;
        long _maximumPage;
        long _currentPageCount;
        long _maximumPageCount;
        long _extensionPageCount;
        long _garbageRootPage;
        long _fetchCounter;
        long _traverseCounter;
        long _storeCounter;
        long _removeCounter;

        VolumeInfo(Volume vol) {
            super();
            _path = vol.getPath();
            _id = vol.getId();
            _createTime = vol.getCreateTime();
            _generation = vol.getGeneration();
            _getCounter = vol.getGetCounter();
            _readCounter = vol.getReadCounter();
            _writeCounter = vol.getWriteCounter();
            _fetchCounter = vol.getFetchCounter();
            _storeCounter = vol.getStoreCounter();
            _removeCounter = vol.getRemoveCounter();
            _traverseCounter = vol.getTraverseCounter();
            _bufferSize = vol.getBufferSize();
            _openTime = vol.getOpenTime();
            _lastRead = vol.getLastReadTime();
            _lastWrite = vol.getLastWriteTime();
            _lastExtension = vol.getLastExtensionTime();
            _maximumPage = vol.getMaximumPageInUse();
            _currentPageCount = vol.getPageCount();
            _maximumPageCount = vol.getMaximumPages();
            _extensionPageCount = vol.getExtensionPages();
            _garbageRootPage = vol.getGarbageRoot();
            _name = vol.getName();
        }

        /**
         * Returns the page size for this <tt>Volume</tt>.
         * 
         * @return the size of each page in this <tt>Volume</tt>
         */
        public int getPageSize() {
            return _bufferSize;
        }

        /**
         * Returns the full path name.
         * 
         * @return the path name.
         */
        public String getPath() {
            return _path;
        }

        /**
         * Returns the alias if one was assigned in the system configuration.
         * 
         * @return the alias for this volume, or <tt>null<tt> if there is none
         */
        public String getName() {
            return _name;
        }

        /**
         * Returns the internal identifier value for this <tt>Volume</tt>
         * 
         * @return the id
         */
        public long getId() {
            return _id;
        }

        /**
         * Returns the time and date when this <tt>Volume</tt> was created.
         * 
         * @return The creation date
         */
        public Date getCreateTime() {
            return new Date(_createTime);
        }

        /**
         * Returns the time and date when this <tt>Volume</tt> was opened.
         * 
         * @return The date when this <tt>Volume</tt> was opened.
         */
        public Date getOpenTime() {
            return new Date(_openTime);
        }

        /**
         * Returns the time and date when the most recent file read occurred.
         * 
         * @return The date when the most recent file read occurred
         */
        public Date getLastReadTime() {
            return new Date(_lastRead);
        }

        /**
         * Returns the time and date when the most recent file write occurred.
         * 
         * @return The date when the most recent file write occurred
         */
        public Date getLastWriteTime() {
            return new Date(_lastWrite);
        }

        /**
         * Returns the time and date when the most recent file extension
         * occurred.
         * 
         * @return The date when the most recent file extension occurred
         */
        public Date getLastExtensionTime() {
            return new Date(_lastExtension);
        }

        /**
         * Returns the generation number for this <tt>Volume</tt>. The
         * generation number increases as the state of the volume changes.
         * 
         * @return The current generation
         */
        public long getGeneration() {
            return _generation;
        }

        /**
         * Returns the number of get operations on this <tt>Volume</tt>. A get
         * operation occurs when a thread attempts to find or update information
         * on the page, regardless of whether the page had previously been
         * copied into the <tt>BufferPool</tt>.
         * 
         * @return the get counter
         */
        public long getGetCounter() {
            return _getCounter;
        }

        /**
         * Returns the number of physical read operations performed against
         * pages in this <tt>Volume</tt>. The read occurs only when Persistit
         * requires the content of a page that has not already been copied into
         * the </tt>BufferPool</tt> or which has become invalid.
         * 
         * @return the read counter
         */
        public long getReadCounter() {
            return _readCounter;
        }

        /**
         * Returns the number of physical write operations performed against
         * pages in this <tt>Volume</tt>.
         * 
         * @return the write counter
         */
        public long getWriteCounter() {
            return _writeCounter;
        }

        /**
         * Returns the count of {@link Exchange#fetch} operations. These include
         * {@link Exchange#fetchAndStore} and {@link Exchange#fetchAndRemove}
         * operations. This count is maintained within the stored Volume and is
         * not reset when Persistit closes. It is provided to assist in
         * application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getFetchCounter() {
            return _fetchCounter;
        }

        /**
         * Returns the count of {@link Exchange#traverse} operations. These
         * include {@link Exchange#next}, {@link Exchange#previous} and other
         * derivative operations. This count is maintained within the stored
         * Volume and is not reset when Persistit closes. It is provided to
         * assist in application performance tuning.
         * 
         * @return The count of key traversal operations performed on this in
         *         this Volume.
         */
        public long getTraverseCounter() {
            return _traverseCounter;
        }

        /**
         * Returns the count of {@link Exchange#store} operations, including
         * {@link Exchange#fetchAndStore} operations. This count is maintained
         * within the stored Volume and is not reset when Persistit closes. It
         * is provided to assist in application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getStoreCounter() {
            return _storeCounter;
        }

        /**
         * Returns the count of {@link Exchange#remove} operations, including
         * {@link Exchange#fetchAndRemove} operations. This count is maintained
         * within the stored Volume and is not reset when Persistit closes. It
         * is provided to assist in application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getRemoveCounter() {
            return _removeCounter;
        }

        /**
         * Returns the current size in bytes of this <tt>Volume</tt>.
         * 
         * @return current size
         */
        public long getCurrentSize() {
            return _currentPageCount * _bufferSize;
        }

        /**
         * Returns the largest page address currently in use within the
         * <tt>Volume</tt>.
         * 
         * @return the largest page address
         */
        public long getMaximumPageAddress() {
            return _maximumPage;
        }

        /**
         * Returns the maximum size in bytes to which this <tt>Volume</tt> may
         * grow.
         * 
         * @return the maximum size
         */
        public long getMaximumSize() {
            return _maximumPageCount * _bufferSize;
        }

        /**
         * Returns the size in bytes by which Persistit will extend this
         * <tt>Volume</tt> when additional file space is required.
         * 
         * @return The extension size
         */
        public long getExtensionSize() {
            return _extensionPageCount * _bufferSize;
        }

        /**
         * Returns the path name of this Volume
         * 
         * @return The path name
         */
        @Override
        public String toString() {
            return _path;
        }

        /**
         * Tests whether the supplied object is a <tt>VolumeInfo</tt> with the
         * same volume name and id as this one.
         * 
         * @return <tt>true</tt> if equal, otherwise <tt>false</tt>
         */
        @Override
        public boolean equals(Object object) {
            return object instanceof VolumeInfo
                    && ((VolumeInfo) object).getId() == _id
                    && ((VolumeInfo) object).getPath().equals(_path);
        }
    }

    /**
     * Exposes information about a Persistit <tt>Tree</tt>.
     */
    public static class TreeInfo extends AcquisitionTimeBase implements
            Serializable {
        public final static long serialVersionUID = -8707513438024673939L;

        String _name;
        int _index;
        long _rootPageAddress;
        int _depth;
        String _volumePathName;
        String _status;
        String _writerThreadName;

        TreeInfo(Tree tree) {
            super();
            _name = tree.getName();
            _index = tree.getTreeIndex();
            _rootPageAddress = tree.getRootPageAddr();
            _depth = tree.getDepth();
            _status = tree.getStatusCode();
            _volumePathName = tree.getVolume().getPath();
            Thread thread = tree.getWriterThread();
            _writerThreadName = thread == null ? null : thread.getName();
        }

        /**
         * Returns the name of the <tt>Tree</tt>
         * 
         * @return the name
         */
        public String getName() {
            return _name;
        }

        /**
         * Returns the index of the <tt>Tree</tt>. Each new tree in
         * <tt>Volume</tt> gets a unique index number.
         * 
         * @return the index
         */
        public int getIndex() {
            return _index;
        }

        /**
         * Returns the page address of the root page.
         * 
         * @return the root page address
         */
        public long getRootPageAddress() {
            return _rootPageAddress;
        }

        /**
         * Returns the count of levels in the <tt>Tree</tt>.
         * 
         * @return the depth
         */
        public int getDepth() {
            return _depth;
        }

        /**
         * Returns the path name of the volume to which this tree belongs.
         * 
         * @return the path name
         */
        public String getVolumePathName() {
            return _volumePathName;
        }

        /**
         * Returns the status code for this Tree.
         * 
         * @return the status code
         */
        public String getStatus() {
            return _status;
        }

        /**
         * Returns the name of the <tt>Thread</tt> that currently holds a writer
         * (exclusive) claim on the <tt>Buffer</tt> if there is one, otherwise
         * returns <tt>null</tt>.
         * 
         * @return The thread name, or <tt>null</tt> if there is none
         */
        public String getWriterThreadName() {
            return _writerThreadName;
        }

        /**
         * Returns the name of the tree.
         * 
         * @return The name of the tree
         */
        @Override
        public String toString() {
            return _name;
        }

        /**
         * Tests whether the supplied object is a <tt>TreeInfo</tt> with the
         * same index and name is this one.
         * 
         * @return <tt>true</tt> if equal, otherwise <tt>false</tt>
         */
        @Override
        public boolean equals(Object object) {
            return object instanceof TreeInfo
                    && ((TreeInfo) object).getIndex() == _index
                    && ((TreeInfo) object).getName().equals(_name);
        }
    }

    /**
     * A structure that contains data about a single logical record within a
     * page. Used by display tools to display the contents of a tree. The method
     * {@link #getLogicalRecordArray} method returns an array of
     * <tt>LogicalRecordInfo</tt> elements for a specified page.
     */
    public static class LogicalRecord extends AcquisitionTimeBase implements
            Serializable {

        public static final long serialVersionUID = -2767943138077168919L;

        //
        // Only used for index and data pages.
        //
        KeyState _key;
        ValueState _value;
        String _keyString;
        String _valueString;

        public KeyState getKeyState() {
            return _key;
        }

        public ValueState getValueState() {
            return _value;
        }

        public String getKeyString() {
            return _keyString;
        }

        public String getValueString() {
            return _valueString;
        }
    }

    /**
     * A structure that returns the result of the {@link #getLogicalRecordCount}
     * method.
     */
    public static class LogicalRecordCount extends AcquisitionTimeBase
            implements Serializable {

        public static final long serialVersionUID = 6028974382268205702L;

        KeyState _keyState;
        int _count;

        LogicalRecordCount(KeyState keyState, int count) {
            super();
            _keyState = keyState;
            _count = count;
        }

        public KeyState getKeyState() {
            return _keyState;
        }

        public int getCount() {
            return _count;
        }
    }

    /**
     * A structure that contains data about a single physical record within a
     * page. Overloaded with information from garbage records. Various
     * diagnostic display tools use this class to hold information extracted
     * from the underlying <tt>Buffer</tt>'s current page. The method
     * {@link #getRecordInfoArray} method returns an array of
     * <tt>RecordInfo</tt> elements for a specified page.
     */
    public static class RecordInfo extends AcquisitionTimeBase implements
            Serializable {
        public final static long serialVersionUID = -3236497038293928535L;
        //
        // Only used for index and data pages.
        //
        int _kbOffset;
        int _tbOffset;
        int _ebc;
        int _db;
        int _klength;
        int _size;
        boolean _inUse;
        KeyState _key;
        ValueState _value;
        //
        // Only used for index pages.
        //
        long _pointerValue;
        //
        // Only for garbage pages.
        //
        int _garbageStatus;
        int _garbageTreeIndex;
        long _garbageLeftPage;
        long _garbageRightPage;

        public int getKbOffset() {
            return _kbOffset;
        }

        public int getTbOffset() {
            return _tbOffset;
        }

        public int getEbc() {
            return _ebc;
        }

        public int getDb() {
            return _db;
        }

        public int getKLength() {
            return _klength;
        }

        public int getSize() {
            return _size;
        }

        public boolean isInUse() {
            return _inUse;
        }

        public KeyState getKeyState() {
            return _key;
        }

        public ValueState getValueState() {
            return _value;
        }

        public long getPointerValue() {
            return _pointerValue;
        }

        public int getGarbageStatus() {
            return _garbageStatus;
        }

        public int getGarbageTreeIndex() {
            return _garbageTreeIndex;
        }

        public long getGarbageLeftPage() {
            return _garbageLeftPage;
        }

        public long getGarbageRightPage() {
            return _garbageRightPage;
        }
    }

    /**
     * Structure that describes the current status of a long-running utility
     * task. See {@link #queryTaskStatus} for further information.
     */
    public static class TaskStatus extends AcquisitionTimeBase implements
            Serializable {
        public static final long serialVersionUID = 8913876646811877035L;

        long _taskId;
        String _description;
        String _owner;
        int _state;
        String _stateName;
        long _startTime;
        long _finishTime;
        long _expirationTime;
        String _statusSummary;
        String _statusDetail;
        String[] _newMessages;
        Exception _lastException;

        /**
         * Returns the unique taskID for this task
         * 
         * @return the unique taskID
         */
        public long getTaskId() {
            return _taskId;
        }

        /**
         * Returns a description of this task
         * 
         * @return the description
         */
        public String getDescription() {
            return _description;
        }

        /**
         * Returns the owner (hostname and/or username) of the initiator of this
         * task
         * 
         * @return the owner
         */
        public String getOwner() {
            return _owner;
        }

        /**
         * Returns the state code for this task. This is one of
         * <ul>
         * <li>{@link Task#STATE_NOT_STARTED}</li>
         * <li>{@link Task#STATE_RUNNING}</li>
         * <li>{@link Task#STATE_SUSPENDED}</li>
         * <li>{@link Task#STATE_DONE}</li>
         * <li>{@link Task#STATE_FAILED}</li>
         * <li>{@link Task#STATE_ENDED}</li>
         * <li>{@link Task#STATE_EXPIRED}</li>
         * </ul>
         * 
         * @return The status
         */
        public int getState() {
            return _state;
        }

        /**
         * Returns the name of the state this task is in. One of "notStarted",
         * "running", "done", "suspended", "failed", "ended", or "expired".
         * 
         * @return A readable name of the task's current state
         */
        public String getStateString() {
            return _stateName;
        }

        /**
         * A short (under 40 character) readable String summarizing the current
         * status.
         * 
         * @return A short readable status
         */
        public String getStatusSummary() {
            return _statusSummary;
        }

        /**
         * A more detailed description of the current status of the task.
         * 
         * @return Detailed status description
         */
        public String getStatusDetail() {
            return _statusDetail;
        }

        /**
         * Messages that have been posted by this task.
         * 
         * @return The messages
         */
        public String[] getMessages() {
            return _newMessages;
        }

        /**
         * System time at which this task was started, or zero if never started.
         * 
         * @return The time
         */
        public long getStartTime() {
            return _startTime;
        }

        /**
         * System time at which this task ended, whether normal or abnormally,
         * or zero if the task has not ended yet.
         * 
         * @return The time
         */
        public long getFinishTime() {
            return _finishTime;
        }

        /**
         * System time at which this task is scheduled to be stopped if it has
         * not finished yet.
         * 
         * @return The time
         */
        public long getExpirationTime() {
            return _expirationTime;
        }

        /**
         * The most recent <tt>Exception</tt> thrown by this task.
         * 
         * @return Most recently thrown <tt>Exception</tt> by this task
         */
        public Exception getLastException() {
            return _lastException;
        }

        /**
         * Tests whether the supplied object is a <tt>TaskStatus</tt> with the
         * same taskId is this one.
         * 
         * @return <tt>true</tt> if equal, otherwise <tt>false</tt>
         */
        @Override
        public boolean equals(Object object) {
            return object instanceof TaskStatus
                    && ((TaskStatus) object).getTaskId() == _taskId;
        }
    }

    public static class JournalInfo extends AcquisitionTimeBase {
        private static final long serialVersionUID = -6628208913672254686L;

        String currentJournalFile;
        long currentJournalAddress;
        long maxJournalFileSize;
        int pageMapSize;
        long currentGeneration;
        long startGeneration;
        long lastValidCheckpointTimestamp;
        long lastValidCheckpointSystemTime;
        String lastValidCheckpointJournalFile;
        long lastValidCheckpointJournalAddress;
        String recoveryJournalFile;
        long recoveryJournalAddress;
        long recoveryStatus;
        long journaledPageCount;
        long copiedPageCount;
        int recoveredCommittedTransactions;
        int recoveredAppliedTransactions;
        boolean closed;
        boolean copying;
        boolean flushing;
        boolean suspendCopying;

        /**
         * @return the current Journal file name
         */
        public String getCurrentJournalFile() {
            return currentJournalFile;
        }

        /**
         * @return the current Journal file address
         */
        public long getCurrentJournalAddress() {
            return currentJournalAddress;
        }

        /**
         * @return the max Journal file size
         */
        public long getMaxJournalFileSize() {
            return maxJournalFileSize;
        }

        /**
         * @return the pageMap size
         */
        public int getPageMapSize() {
            return pageMapSize;
        }

        /**
         * @return the current generation
         */
        public long getCurrentGeneration() {
            return currentGeneration;
        }

        /**
         * Start generation is the generation of the earliest journal file
         * containing pages or records not yet copied to Volumes.
         * 
         * @return the start generation
         */
        public long getStartGeneration() {
            return startGeneration;
        }

        /**
         * @return the time stamp of latest valid Checkpoint
         */
        public long getLastValidCheckpointTimestamp() {
            return lastValidCheckpointTimestamp;
        }

        /**
         * @return the system time (in millis) of the latest valid Checkpoint
         */
        public long getLastValidCheckpointSystemTime() {
            return lastValidCheckpointSystemTime;
        }

        /**
         * Convenience method to return the elapsed time since the last valid
         * checkpoint was written.
         * 
         * @return the elapsed time in seconds since the latest valid Checkpoint
         */
        public long getLastValidCheckpointAge() {
            return (System.currentTimeMillis() - lastValidCheckpointSystemTime + 500) / 1000;
        }

        /**
         * @return the name of the journal file to which the last valid
         *         checkpoint record was written
         */
        public String getLastValidCheckpointJournalFile() {
            return lastValidCheckpointJournalFile;
        }

        /**
         * @return the file address of the last valid checkpoint record
         */
        public long getLastValidCheckpointJournalAddress() {
            return lastValidCheckpointJournalAddress;
        }

        /**
         * The current journal file being recovered during startup. After
         * recovery is compete, the name of the last journal file used during
         * recovery.
         * 
         * @return the journal file from which a recovery operation was last
         *         performed.
         */
        public String getRecoveryJournalFile() {
            return recoveryJournalFile;
        }

        /**
         * Address within journal file currently being recovered during startup.
         * After recovery is complete, the address after the last recovered
         * record.
         * 
         * @return the current recovery file address.
         */
        public long getRecoveryJournalAddress() {
            return recoveryJournalAddress;
        }

        /**
         * Return one of the following values:
         * <p />
         * <table>
         * <tr>
         * <td>Long.MIN_VALUE</td>
         * <td>if recovery has not begun yet</td>
         * </tr>
         * <tr>
         * <td>-1</td>
         * <td>if recovery ended after a "dirty" (abruptly terminated) shutdown</td>
         * </tr>
         * <tr>
         * <td>0</td>
         * <td>if recovery completed after a "clean" (normal) shutdown</td>
         * </tr>
         * <tr>
         * <td>N &gt; 0</td>
         * <td>if recovery is in progress, in which case N represents the
         * approximate number of remaining bytes of journal data to process.</td>
         * </tr>
         * </table>
         * 
         * @return the recovery status.
         */
        public long getRecoveryStatus() {
            return recoveryStatus;
        }

        /**
         * @return the total number of pages written to the journal
         */
        public long getJournaledPageCount() {
            return journaledPageCount;
        }

        /**
         * @return the total number of pages copied from the journal back to
         *         their Volumes.
         */
        public long getCopiedPageCount() {
            return copiedPageCount;
        }
        
        /**
         * @return the count of recovered uncommitted transactions
         */
        public int getRecoveredCompletedTransactions() {
            return recoveredCommittedTransactions;
        }

        /**
         * @return the count of recovered committed Transactions
         */
        public int getRecoveredCommittedTransactions() {
            return recoveredAppliedTransactions;
        }


        /**
         * @return <tt>true</tt> iff the journal has been closed
         */
        public boolean isClosed() {
            return closed;
        }

        /**
         * @return <tt>true</tt> iff the JOURNAL_COPIER task is currently
         *         copying pages to their Volume files.
         */
        public boolean isCopying() {
            return copying;
        }

        /**
         * @return <tt>true</tt> iff the JOURNAL_FLUSHER task is currently
         *         forcing file modifications to disk.
         */
        public boolean isFlushing() {
            return flushing;
        }

        /**
         * The suspendCopying flag suspends copying of pages to their Volume
         * files.
         * 
         * @return <tt>true</tt> if the suspendCopying flag is set.
         */
        public boolean isSuspendCopying() {
            return suspendCopying;
        }

    }

    /**
     * A subclass of <tt>RemoteException</tt> that wraps a <tt>Throwable</tt> so
     * that it can be returned to the remote client.
     */
    public static class WrappedRemoteException extends RemoteException {

        public static final long serialVersionUID = 3297945951883397630L;

        Throwable _cause;

        public WrappedRemoteException(Throwable cause) {
            _cause = cause;
        }

        @Override
        public Throwable getCause() {
            return _cause;
        }
    }

}
