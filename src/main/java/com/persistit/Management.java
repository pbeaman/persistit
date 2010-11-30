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
public interface Management extends Remote, ManagementMXBean {
    /**
     * Indicates whether Persistit is currently in the initialized state.
     * 
     * @return The state
     */
    public boolean isInitialized() throws RemoteException;

    /**
     * Returns the version name of the current Persistit instance.
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
     * Controls whether Persistit suspends the thread that copies pages from the
     * journal back to their respective Volumes. This flag is used by tools that
     * provide on-line backup.
     * 
     * @param suspended
     *            <tt>true</tt> to specify that Persistit will suspend journal
     *            copying; otherwise <tt>false</tt>.
     */
    public void setAppendOnly(boolean suspended) throws RemoteException;

    /**
     * Controls whether Persistit copies page from the journal back to their
     * volumes as fast as possible. Copying consumes disk I/O operations, so
     * normally the copier thread pauses between copy operations to avoid
     * saturating the disk. Once all pages have been copied, the fast copying
     * flag is automatically turned off.
     * 
     * @param fast
     *            <tt>true</tt> to copy pages at maximum speed.
     * @throws RemoteException
     */
    public void setJournalCopyingFast(boolean fast) throws RemoteException;

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
     * Returns a {@link JournalInfo} structure describing the current state of
     * the Journal.
     * 
     * @return the JournalInfo
     * @throws RemoteException
     */
    public JournalInfo getJournalInfo() throws RemoteException;

    /**
     * Returns a {@link RecoveryInfo} structure describing the current state of
     * the recovery process.
     * 
     * @return
     * @throws RemoteException
     */
    public RecoveryInfo getRecoveryInfo() throws RemoteException;

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

}
