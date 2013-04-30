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

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.Map;

import javax.management.ObjectName;

import com.persistit.encoding.CoderContext;
import com.persistit.mxbeans.ManagementMXBean;
import com.persistit.util.Util;

/**
 * <p>
 * Interface for a service object that exposes information about the Persistit
 * environment. With this public API, embedding applications can query
 * performance metrics and resources within Persistit that are not exposed by
 * the normal access methods. For example, this class provides methods that
 * enumerate the volumes currently mounted, the size and effectiveness of buffer
 * pool components, and many other items that may be useful in tuning and
 * managing Persistit.
 * </p>
 * <p>
 * Methods of this class are exposed via RMI (Remote Method Invocation) to
 * permit management tools to operate remotely. For example, the AdminUI tool
 * can connect to a running Persistit instance via RMI. Most of the information
 * available on this interface is also accessible through JMX MXBeans.
 * </p>
 * 
 * @version 1.0
 */
public interface Management extends Remote, ManagementMXBean {
    /**
     * Controls whether Persistit suspends the thread that copies pages from the
     * journal back to their respective Volumes. This flag is used by tools that
     * provide on-line backup.
     * 
     * @param suspended
     *            <code>true</code> to specify that Persistit will suspend
     *            journal copying; otherwise <code>false</code>.
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
     *            <code>true</code> to copy pages at maximum speed.
     * @throws RemoteException
     */
    public void setJournalCopyingFast(boolean fast) throws RemoteException;

    /**
     * Attempts to close Persistit by invoking {@link Persistit#close}.
     * 
     * @return <code>true</code> if the attempt to close Persistit was
     *         successful; otherwise <code>false</code>
     * @throws RemoteException
     */
    public boolean close() throws RemoteException;

    /**
     * Return an array containing a <code>BufferPoolInfo</code> element for each
     * buffer pool. If Persistit is not initialized then this method returns an
     * empty array.
     * 
     * @return The array
     */
    public BufferPoolInfo[] getBufferPoolInfoArray() throws RemoteException;

    /**
     * Return an array containing a <code>RecordInfo</code> element for each
     * record in the page specified by <code>volumeName</code> and
     * <code>pageAddress</code>. If Persistit is not initialized, or if there is
     * no unique <code>Volume</code> for the specified <code>volumeName</code>,
     * or if there is no page associated with the specified
     * <code>pageAddress</code> or if there is any transient condition that
     * causes the attempt to retrieve records to fail, then this method returns
     * an empty array.
     * 
     * @param volumeName
     *            The volume name
     * 
     * @param pageAddress
     *            The page addres
     * 
     * @return the array Array of <code>RecordInfo</code> objects containing
     *         detailed information about the records on the specified page.
     */
    public RecordInfo[] getRecordInfoArray(String volumeName, long pageAddress) throws RemoteException;

    /**
     * <p>
     * Return an array of <code>LogicalRecord</code> elements from a
     * <code>Tree</code>. The tree is specified by the provided
     * <code>volumeName</code> and <code>treeName</code> values. The size of the
     * the returned array is bounded by <code>maxRecordCount</code>. Records
     * whose <code>Value</code> fields exceed <code>maxValueBytes</code> in
     * length are truncated to that size. This allows a client program to limit
     * the maximum number of bytes to be transmitted in one RMI request. If non-
     * <code>null</code>, <code>keyFilterString</code> specifies a
     * <code>KeyFilter</code> to be applied when taversing records. The supplied
     * <code>fromKey</code> specifies a starting key value, and is non-
     * inclusive.
     * </p>
     * <p>
     * Each <code>LogicalRecord</code> object returned by this method optionally
     * contain decoded String representations of the record's key and value. If
     * <code>decodeString</code> is <code>true</code> then each
     * <code>LogicalRecord</code>'s <code>KeyState</code>,
     * <code>KeyString</code> and <code>ValueString</code> properties are set to
     * String values resulting from decoding the underlying key and value in the
     * context of the running Persistit instance. Otherwise,
     * <code>KeyString</code> and <code>ValueString</code> are <code>null</code>
     * . Applications that display String-valued representations of a record's
     * key and value should typically enable <code>decodeString</code> so that
     * any required {@link com.persistit.encoding.KeyCoder} or
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
     *            If non-<code>null</code>, specifies the String representation
     *            of a {@link KeyFilter}. Only records having key values
     *            selected by this filter are returned.
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
     *            If <code>true</code>, decode each <code>LogicalRecord</code>'s
     *            <code>Key</code> and <code>Value</code> as Strings.
     * 
     * @return Array of LogicalRecord objects
     * 
     * @throws RemoteException
     */
    public LogicalRecord[] getLogicalRecordArray(String volumeName, String treeName, String keyFilterString,
            KeyState fromKey, Key.Direction direction, int maxRecordCount, int maxValueBytes, boolean decodeStrings)
            throws RemoteException;

    /**
     * Return a {@link JournalInfo} structure describing the current state of
     * the Journal.
     * 
     * @return the JournalInfo
     * @throws RemoteException
     */
    public JournalInfo getJournalInfo() throws RemoteException;

    /**
     * Return a {@link RecoveryInfo} structure describing the current state of
     * the recovery process.
     * 
     * @return the JournalInfo
     * @throws RemoteException
     */
    public RecoveryInfo getRecoveryInfo() throws RemoteException;

    /**
     * Return a {@link TransactionInfo} structure summarizing Transaction commit
     * and rollback information.
     * 
     * @return the TransactionInfo
     * @throws RemoteException
     */
    public TransactionInfo getTransactionInfo() throws RemoteException;

    /**
     * <p>
     * Count the the number of records that could be traversed given a starting
     * key value in a tree specified by <code>volumeName</code>,
     * <code>treeName</code>, using an optional {@link KeyFilter} specified by
     * <code>keyFilterString</code>. Records are counted by traversing forward
     * or backward according to the Persistit <a
     * href="Key.html#_keyOrdering">key order specification</a>. The direction
     * of traversal is specified by <code>direction</code>.
     * </p>
     * <p>
     * The returned <code>LogicalRecordCount</code> contains the count of
     * records actually traversed by this method, which will never exceed
     * <code>maximumCount</code>. It also contains a <code>KeyState</code>
     * representing the value of the <code>Key</code> of the final record
     * traversed by this method. If the returned count is N then the returned
     * <code>KeyState</code> corresponds with the Nth record counted.
     * </p>
     * 
     * @param volumeName
     *            The volume name
     * 
     * @param treeName
     *            The tree name
     * 
     * @param keyFilterString
     *            If non-<code>null</code>, specifies the String representation
     *            of a {@link KeyFilter}. Only records having key values
     *            selected by this filter are returned.
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

    public LogicalRecordCount getLogicalRecordCount(String volumeName, String treeName, String keyFilterString,
            KeyState fromKey, Key.Direction direction, int maximumCount) throws RemoteException;

    /**
     * <p>
     * Return an array of {@link BufferInfo} objects reflecting the states of
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
    public BufferInfo[] getBufferInfoArray(int bufferSize, int traversalType, String includeMask, String excludeMask)
            throws RemoteException;

    /**
     * Return a <code>BufferInfo</code> reflecting the status of the buffer
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
    public BufferInfo getBufferInfo(String volumeName, long pageAddress) throws RemoteException;

    /**
     * Return a <code>BufferInfo</code> reflecting the state of a page
     * containing the specified key. The <code>volumeName</code> and
     * <code>treeName</code> parameters specify a {@link Tree} in which to seach
     * for the key. The <code>level</code> parameter indicates whether the data
     * page, or one of the pages on the index path to that data page should be
     * returned. Level 0 refers to the data path, level 1 is the lowest index
     * level, and level d-1 where d is the number of levels in the the tree
     * represents the three's root page.
     * <p>
     * Specify <code>treeName</code> as <code>null</code> to access the volume's
     * directory tree.
     * 
     * @param volumeName
     *            the name of the volume
     * @param treeName
     *            the name of the tree within the volume, or <code>null</code>
     *            for the directory tree
     * @param key
     *            a <code>KeyState</code> representing a key
     * @param level
     *            tree level: 0 for root, 1...d-1 for index pages of a tree
     *            having depth d.
     * @return a <code>BufferInfo</code> object reflecting the selected page, or
     *         <code>null</code> if the specified tree does not exist.
     * @throws RemoteException
     */
    public BufferInfo getBufferInfo(String volumeName, String treeName, KeyState key, int level) throws RemoteException;

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
    public int populateBufferInfoArray(BufferInfo[] results, int bufferSize, int traversalType, String includeMask,
            String excludeMask) throws RemoteException;

    /**
     * Return an array containing a <code>VolumeInfo</code> element for each
     * open volume. If Persistit is not initialized then this method returns an
     * empty array. </p>
     * 
     * @return The array
     */
    public VolumeInfo[] getVolumeInfoArray() throws RemoteException;

    /**
     * Return a Class definition for a class specified by its name. This allows
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
    public Class<?> getRemoteClass(String className) throws RemoteException;

    /**
     * Return the <code>VolumeInfo</code> for the volume specified by the
     * supplied <code>volumeName</code>. If Persisit is not initialized or there
     * is no unique volume corresponding with the supplied name, then this
     * method returns <code>null</code>.
     * 
     * @param volumeName
     * 
     * @return the <code>VolumeInfo</code>
     */
    public VolumeInfo getVolumeInfo(String volumeName) throws RemoteException;

    /**
     * Return an array containing a <code>TreeInfo</code> element for each
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
    public TreeInfo[] getTreeInfoArray(String volumeName) throws RemoteException;

    /**
     * Return a <code>TreeInfo</code> for a specified <code>Volume</code> and
     * <code>Tree</code>. If Persisit is not initialized, or if no no volume or
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
    public TreeInfo getTreeInfo(String volumeName, String treeName) throws RemoteException;

    /**
     * Parse the supply String to determine whether it is a valid
     * <code>KeyFilter</code> and return the index of the first incorrect
     * character in the supplied String, or -1 if the string is a valid String
     * representation of a KeyFilter.
     * 
     * @param keyFilterString
     * 
     * @return index of first invalid character in the supplied
     *         <code>keyFilterString</code>, or -1 if the string is valid.
     * 
     * @throws RemoteException
     */
    public int parseKeyFilterString(String keyFilterString) throws RemoteException;

    /**
     * <p>
     * Decodes the content of the supplied <code>ValueState</code> as an array
     * of Objects. Usually this array has one element containing the single
     * object value encoded in the <code>ValueState</code>. However, if multiple
     * items were written to the original <code>Value</code> from which the
     * <code>ValueState</code> was derived in <a
     * href="Value.html#_streamMode">Stream Mode</a>, this method returns all of
     * the encoded objects.
     * </p>
     * <p>
     * If the <code>valueState</code> represents an undefined value, this method
     * returns an array of length zero. If the <code>valueState</code> encodes a
     * value of <code>null</code>, then this method returns an array containing
     * one element which is <code>null</code>.
     * </p>
     * 
     * @param valueState
     *            Representation of an encoded {@link Value}.
     * 
     * @param context
     *            Object passed to any {@link com.persistit.encoding.ValueCoder}
     *            used in decoding the value. May be <code>null</code>.
     * 
     * @return Array of zero or more decoded objects
     * @throws RemoteException
     */
    public Object[] decodeValueObjects(ValueState valueState, CoderContext context) throws RemoteException;

    /**
     * Decodes the content of the supplied <code>KeyState</code> as an array of
     * Objects, one object per <a href="Key.html#_keySegments"> key segment</a>.
     * 
     * @param keyState
     *            Representation of an encoded {@link Key}.
     * 
     * @param context
     *            Object passed to any {@link com.persistit.encoding.KeyCoder}
     *            used in decoding the value. May be <code>null</code>
     * 
     * @return Array of decoded key segments
     * 
     * @throws RemoteException
     */
    public Object[] decodeKeyObjects(KeyState keyState, CoderContext context) throws RemoteException;

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
     * @param commandLine
     *            command name followed by task-specific parameters delimited by
     *            spaces.
     * @param maximumTime
     *            Maximum wall-clock time (in milliseconds) this Task will be
     *            allowed to run
     * @param verbosity
     *            Verbosity level, one of {@link Task#LOG_NORMAL} or
     *            {@link Task#LOG_VERBOSE}.
     * @return Task identifier Unique ID for the running task
     * @throws RemoteException
     */
    public long startTask(String description, String owner, String commandLine, long maximumTime, int verbosity)
            throws RemoteException;

    /**
     * Launch a task.
     * 
     * @param task
     *            the Task
     * @param description
     *            description identifying what the task is doing
     * @return the taskId as a String, or other status information
     * @throws RemoteException
     */
    public String launch(final Task task, final String description) throws RemoteException;

    /**
     * Queries the current status of one or all tasks. If the specified taskId
     * value is -1, this method returns status information for all currently
     * active tasks. This method clears all completed tasks after reporting
     * their final status.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param details
     *            <code>true</code> to populate each returned
     *            <code>TaskStatus</code> object with all new messages posted by
     *            the task.
     * @param clearMessages
     *            <code>true</code> to clear all received messages from the
     *            task.
     * 
     * @return Array of <code>TaskStatus</code> objects indicating status of
     *         selected task(s).
     * @throws RemoteException
     */
    public TaskStatus[] queryTaskStatus(long taskId, boolean details, boolean clearMessages) throws RemoteException;

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
     * @param clearMessages
     *            <code>true</code> to clear all received messages from the
     *            task.
     * @param clearTasks
     *            <code>true</code> to clear all completed tasks after reporting
     *            status
     * @return Array of <code>TaskStatus</code> objects indicating status of
     *         selected task(s).
     * @throws RemoteException
     */
    public TaskStatus[] queryTaskStatus(long taskId, boolean details, boolean clearMessages, boolean clearTasks)
            throws RemoteException;

    /**
     * Suspend or resume the task(s) identified by <code>taskId</code>. If
     * <code>taskId</code> is -1, all tasks are modified.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @param suspend
     *            <code>true</code> to suspend the task, <code>false</code> to
     *            allow it to resume.
     * @throws RemoteException
     */
    public void setTaskSuspended(long taskId, boolean suspend) throws RemoteException;

    /**
     * Stops and optionally removes a task specified by its id value. If the
     * task is currently running, this method stops it. If <code>remove</code>
     * is <code>true</code> this method also removes the Task from the task
     * list. Otherwise the task remains on the task list in the
     * {@link Task#STATE_ENDED} state. If <code>taskId</code> is -1 then these
     * actions are applied to all tasks.
     * 
     * @param taskId
     *            Task ID for a selected Task, or -1 for all Tasks.
     * @throws RemoteException
     */
    public void stopTask(long taskId, boolean remove) throws RemoteException;

    /**
     * Removes done, expired, stopped or failed tasks from the task list. This
     * method does not affected running or suspended tasks.
     * 
     * @param taskId
     *            Task ID for a selected task, or -1 for all tasks.
     * 
     * @throws RemoteException
     */
    public void removeFinishedTasks(long taskId) throws RemoteException;

    public DisplayFilter getDisplayFilter() throws RemoteException;

    public void setDisplayFilter(DisplayFilter displayFilter) throws RemoteException;

    /**
     * Return an unmodifiable map of all MXBeans registered in the Persistit
     * instance. The map key is the ObjectName used in identifying the MXBean
     * within the JMX framework. For most of the MXBean instance you can
     * construct the ObjectName from a constant in the interface. for example
     * 
     * <code><pre>
     *    ObjectName on = 
     *        new ObjectName(JournalManagerMXBean.MXBEAN_NAME);
     * </pre></code>
     * 
     * For BufferPoolMXBean instances, use
     * 
     * <code><pre>
     *    ObjectName on = 
     *        new ObjectName(BufferPoolMXBean.MXBEAN_NAME + "." + bufferSize);
     * </pre></code>
     * 
     * 
     * @return Map of ObjectName to Object for all registered MXBeans
     */
    Map<ObjectName, Object> getMXBeans() throws RemoteException;

    /**
     * A DisplayFilter formats the String value presented in the Tree tab of the
     * AdminUI. Invoke {@link ManagementImpl#setDisplayFilter(DisplayFilter)} to
     * supply a non-default DisplayFilter.
     * 
     * @author peter
     * 
     */
    public interface DisplayFilter {

        public String toKeyDisplayString(final Exchange exchange);

        public String toValueDisplayString(final Exchange exchange);

    }

    /**
     * A structure that contains data about a single logical record within a
     * page. Used by display tools to display the contents of a tree. The method
     * {@link #getLogicalRecordArray} method returns an array of
     * <code>LogicalRecordInfo</code> elements for a specified page.
     */
    public static class LogicalRecord extends AcquisitionTimeBase implements Serializable {

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
    public static class LogicalRecordCount extends AcquisitionTimeBase implements Serializable {

        public static final long serialVersionUID = 6028974382268205702L;

        KeyState _keyState;
        int _count;

        LogicalRecordCount(final KeyState keyState, final int count) {
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
     * from the underlying <code>Buffer</code>'s current page. The method
     * {@link #getRecordInfoArray} method returns an array of
     * <code>RecordInfo</code> elements for a specified page.
     */
    public static class RecordInfo extends AcquisitionTimeBase implements Serializable {
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
        String _error;
        //
        // Only used for index pages.
        //
        long _pointerValue;
        //
        // Only for garbage pages.
        //
        int _garbageStatus;
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

        public long getGarbageLeftPage() {
            return _garbageLeftPage;
        }

        public long getGarbageRightPage() {
            return _garbageRightPage;
        }

        public String getError() {
            return _error;
        }
    }

    /**
     * Base class for all management data structures that retain the time at
     * which the information was recorded.
     */
    abstract static class AcquisitionTimeBase implements Serializable {

        private static final long serialVersionUID = 5162380870350766401L;

        long time;

        private AcquisitionTimeBase() {
            updateAcquisitonTime();
        }

        /**
         * Return the time (in milliseconds since January 1, 1970 00:00:00 GMT)
         * at which the information represented by this object was acquired.
         * 
         * @return the information acquisition time
         */
        public long getAcquisitionTime() {
            return time;
        }

        public void setAcquisitionTime(final long time) {
            this.time = time;
        }

        void updateAcquisitonTime() {
            time = System.currentTimeMillis();
        }
    }

    /**
     * Exposes information about a BufferPool. A BufferPool is a container for
     * all the buffers of a particular size. The {@link #getBufferPoolInfoArray}
     * method returns an array containing BufferPoolInfo elements for each
     * buffer size.
     */
    public static class BufferPoolInfo extends AcquisitionTimeBase implements Serializable {
        public final static long serialVersionUID = 9044282840712593435L;

        int bufferSize;
        int bufferCount;
        long missCount;
        long hitCount;
        long newCount;
        long evictCount;
        long writeCount;
        long forcedWriteCount;
        long forcedCheckpointWriteCount;
        int validPageCount;
        int dirtyPageCount;
        int readerClaimedPageCount;
        int writerClaimedPageCount;
        long earliestDirtyTimestamp;

        public BufferPoolInfo() {

        }

        @ConstructorProperties({ "bufferSize", "bufferCount", "missCount", "hitCount", "newCount", "evictCount",
                "writeCount", "forcedWriteCount", "forcedCheckpointWriteCount", "validPageCount", "dirtyPageCount",
                "readerClaimedPageCount", "writerClaimedPageCount", "earliestDirtyTimestamp" })
        public BufferPoolInfo(final int bufferSize, final int bufferCount, final long missCount, final long hitCount,
                final long newCount, final long writeCount, final long evictCount, final long forcedWriteCount,
                final long forcedCheckpointWriteCount, final long readCounter, final int validPageCount,
                final int dirtyPageCount, final int readerClaimedPageCount, final int writerClaimedPageCount,
                final long earliestDirtyTimestamp) {
            super();
            this.bufferSize = bufferSize;
            this.bufferCount = bufferCount;
            this.missCount = missCount;
            this.hitCount = hitCount;
            this.newCount = newCount;
            this.evictCount = evictCount;
            this.writeCount = writeCount;
            this.forcedWriteCount = forcedWriteCount;
            this.forcedCheckpointWriteCount = forcedCheckpointWriteCount;
            this.validPageCount = validPageCount;
            this.dirtyPageCount = dirtyPageCount;
            this.readerClaimedPageCount = readerClaimedPageCount;
            this.writerClaimedPageCount = writerClaimedPageCount;
            this.earliestDirtyTimestamp = earliestDirtyTimestamp;
        }

        /**
         * Return the size of <code>Buffer</code>s managed by this pool.
         * 
         * @return The size in bytes of each buffer in this pool
         */
        public int getBufferSize() {
            return bufferSize;
        }

        /**
         * Return the count of <code>Buffer</code>s managed by this pool.
         * 
         * @return The count
         */
        public int getBufferCount() {
            return bufferCount;
        }

        /**
         * Return the count of lookup operations for pages images in this pool.
         * This number, in comparison with the hit counter, indicates how
         * effective the cache is in reducing disk I/O.
         * 
         * @return The get count
         */
        public long getMissCount() {
            return missCount;
        }

        /**
         * Return the count of lookup operations for pages images in this pool
         * for which the page image was already found in this
         * <code>BufferPool</code>. This number, in comparison with the miss
         * counter, indicates how effective the cache is in reducing disk I/O.
         * 
         * @return The hit count
         */
        public long getHitCount() {
            return hitCount;
        }

        /**
         * Return the count of newly create pages images in this Pool. A new
         * page is one that does not need to be read from disk.
         * 
         * @return The new page counter
         */
        public long getNewCount() {
            return newCount;
        }

        /**
         * Return the count of valid pages evicted from this pool.
         * 
         * @return The evicted page count
         */
        public long getEvictCount() {
            return evictCount;
        }

        /**
         * Return count of pages written from this pool.
         * 
         * @return The forced write count
         */
        public long getWriteCount() {
            return writeCount;
        }

        /**
         * Return count of pages forced to be written when dirty on eviction
         * 
         * @return The forced write count
         */
        public long getForcedWriteCount() {
            return forcedWriteCount;
        }

        /**
         * Return count of pages forced to be written due to an updated after a
         * checkpoint
         * 
         * @return The forced checkpoint write count
         */
        public long getForcedCheckpointWriteCount() {
            return forcedCheckpointWriteCount;
        }

        /**
         * Get the "hit ratio" - the number of hits divided by the number of
         * overall gets. A value close to 1.0 indicates that most attempts to
         * find data in the <code>BufferPool</code> are successful - i.e., that
         * the cache is effectively reducing the need for disk read operations.
         * 
         * @return The ratio
         */
        public double getHitRatio() {
            final long denominator = missCount + hitCount + newCount;
            if (denominator == 0)
                return 0.0;
            else
                return ((double) hitCount) / ((double) denominator);
        }

        /**
         * Get the count of valid pages in this pool.
         * 
         * @return The count of valid pages in this pool
         */
        public int getValidPageCount() {
            return validPageCount;
        }

        /**
         * Get the count of dirty pages (pages that contain updates not yet
         * written to disk) in this pool.
         * 
         * @return The count of dirty pages in this pool
         */
        public int getDirtyPageCount() {
            return dirtyPageCount;
        }

        /**
         * Get the count of pages on which running threads have reader
         * (non-exclusive), but <i>not</i> writer (exclusive) claims in this
         * pool.
         * 
         * @return The count of pages with reader claims
         */
        public int getReaderClaimedPageCount() {
            return readerClaimedPageCount;
        }

        /**
         * Get the count of pages on which running threads have writer
         * (exclusive) claims in this pool.
         * 
         * @return The count of pages with writer claims
         */
        public int getWriterClaimedPageCount() {
            return writerClaimedPageCount;
        }

        /**
         * @return Earliest timestamp of any dirty page in this
         *         <code>BufferPool</code>
         */
        public long getEarliestDirtyTimestamp() {
            return earliestDirtyTimestamp;
        }
    }

    /**
     * <p>
     * Exposes information about one <code>Buffer</code>. Use the
     * {@link #getBufferInfoArray} method to get an array of
     * <code>BufferInfo</code> s for <code>Buffers</code> in various states.
     * </p>
     * <p>
     * Note that the content of a <code>BufferPool</code> and the status of
     * individual <code>Buffer</code>s changes extremely rapidly when Persistit
     * is active. A <code>BufferInfo</code> represents a snapshot of this state
     * at the acquisition time.
     * </p>
     */
    public static class BufferInfo extends AcquisitionTimeBase implements Serializable {
        public final static long serialVersionUID = -7847730868509629017L;

        int poolIndex;
        int type;
        String typeName;
        int status;
        String statusName;
        String writerThreadName;
        long pageAddress;
        long rightSiblingAddress;
        String volumeName;
        long volumeId;
        long timestamp;
        int bufferSize;
        int availableBytes;
        int alloc;
        int slack;
        int mvvCount;
        int mvvSize;
        int keyBlockStart;
        int keyBlockEnd;

        public BufferInfo() {

        }

        @ConstructorProperties({ "poolIndex", "type", "typeName", "status", "statusName", "writerThreadName",
                "pageAddress", "rightSiblingAddress", "volumeName", "volumeId", "timestamp", "bufferSize",
                "availableBytes", "alloc", "slack", "mvvCount", "mvvSize", "keyBlockStart", "keyBlockEnd" })
        public BufferInfo(final int poolIndex, final int type, final String typeName, final int status,
                final String statusName, final String writerThreadName, final long pageAddress,
                final long rightSiblingAddress, final String volumeName, final long volumeId, final long timestamp,
                final int bufferSize, final int availableBytes, final int alloc, final int slack, final int mvvCount,
                final int mvvSize, final int keyBlockStart, final int keyBlockEnd) {
            super();
            this.poolIndex = poolIndex;
            this.type = type;
            this.typeName = typeName;
            this.status = status;
            this.statusName = statusName;
            this.writerThreadName = writerThreadName;
            this.pageAddress = pageAddress;
            this.rightSiblingAddress = rightSiblingAddress;
            this.volumeName = volumeName;
            this.volumeId = volumeId;
            this.timestamp = timestamp;
            this.bufferSize = bufferSize;
            this.availableBytes = availableBytes;
            this.alloc = alloc;
            this.slack = slack;
            this.mvvCount = mvvCount;
            this.mvvSize = mvvSize;
            this.keyBlockStart = keyBlockStart;
            this.keyBlockEnd = keyBlockEnd;
        }

        /**
         * Indicates whether the page occupying this buffer is a Garbage page.
         * 
         * @return <code>true</code> if this is a Garbage page, otherwise
         *         <code>false</code>.
         */
        public boolean isGarbagePage() {
            return type == Buffer.PAGE_TYPE_GARBAGE;
        }

        /**
         * Indicates whether the page occupying this buffer is a Data page.
         * 
         * @return <code>true</code> if this is a Data page, otherwise
         *         <code>false</code>.
         */
        public boolean isDataPage() {
            return type == Buffer.PAGE_TYPE_DATA;
        }

        /**
         * Indicates whether the page occupying this buffer is an Index page.
         * 
         * @return <code>true</code> if this is an Index page, otherwise
         *         <code>false</code>.
         */
        public boolean isIndexPage() {
            return type >= Buffer.PAGE_TYPE_INDEX_MIN && type <= Buffer.PAGE_TYPE_INDEX_MAX;
        }

        /**
         * Indicates whether the page occupying this buffer is a LongRecord
         * page.
         * 
         * @return <code>true</code> if this is a LongRecord page, otherwise
         *         <code>false</code>.
         */
        public boolean isLongRecordPage() {
            return type == Buffer.PAGE_TYPE_LONG_RECORD;
        }

        /**
         * Indicates whether the page occupying this buffer is an Unallocated
         * page.
         * 
         * @return <code>true</code> if this is an Unallocated page, otherwise
         *         <code>false</code>.
         */
        public boolean isUnallocatedPage() {
            return type == Buffer.PAGE_TYPE_UNALLOCATED;
        }

        /**
         * Return the index of this <code>Buffer</code> within the
         * <code>BufferPool</code> array.
         * 
         * @return The index
         */
        public int getPoolIndex() {
            return poolIndex;
        }

        /**
         * Return the type of page occupying this <code>Buffer</code> as an
         * integer. See {@link #getTypeName} for a displayable form.
         * 
         * @return The type.
         */
        public int getType() {
            return type;
        }

        /**
         * Return the type of page occupying this <code>Buffer</code> in
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
            return typeName;
        }

        /**
         * Return the status integer for this <code>Buffer</code>. See
         * {@link #getStatusName} for a displayable form.
         * 
         * @return The status integer.
         */
        public int getStatus() {
            return status;
        }

        /**
         * Return the status of this <code>Buffer</code> in displayable form.
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
            return statusName;
        }

        /**
         * Return the name of the <code>Thread</code> that currently holds a
         * writer (exclusive) claim on the <code>Buffer</code> if there is one,
         * otherwise returns <code>null</code>.
         * 
         * @return The thread name, or <code>null</code> if there is none
         */
        public String getWriterThreadName() {
            return writerThreadName;
        }

        /**
         * @return The page address.
         */
        public long getPageAddress() {
            return pageAddress;
        }

        /**
         * @return The page address of the right sibling page.
         */
        public long getRightSiblingAddress() {
            return rightSiblingAddress;
        }

        /**
         * @return the Volume Name
         */
        public String getVolumeName() {
            return volumeName;
        }

        /**
         * @return the volume ID
         */
        public long getVolumeId() {
            return volumeId;
        }

        /**
         * @return the timestamp at which the page occupying this buffer was
         *         last changed.
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * @return the buffer size, in bytes
         */
        public int getBufferSize() {
            return bufferSize;
        }

        /**
         * @return the number of unused bytes available to hold additional
         *         key/value pairs.
         */
        public int getAvailableBytes() {
            return availableBytes;
        }

        /**
         * @return he offset within the <code>Buffer</code> used internally in
         *         allocating space for key/value pairs.
         */
        public int getAlloc() {
            return alloc;
        }

        /**
         * @return a size used internally in allocating space for key/value
         *         pairs.
         */
        public int getSlack() {
            return slack;
        }

        /**
         * @return the approximate count of key/value pairs containing
         *         multi-version values.
         */
        public int getMvvCount() {
            return mvvCount;
        }

        /**
         * @return the approximate number of bytes consumed by multi-version
         *         values which are no longer visible to current transactions.
         */
        public int getMvvSize() {
            return mvvSize;
        }

        /**
         * Return an offset within the <code>Buffer</code> used internally in
         * allocating space for key/value pairs.
         * 
         * @return The keyBlockStart offset
         */
        public int getKeyBlockStart() {
            return keyBlockStart;
        }

        /**
         * Return an offset within the <code>Buffer</code> used internally in
         * allocating space for key/value pairs.
         * 
         * @return The keyBlockEnd offset
         */
        public int getKeyBlockEnd() {
            return keyBlockEnd;
        }

        /**
         * Return a human-readable summary of information about the page
         * contained in this <code>Buffer</code>.
         * 
         * @return a one-line displayable summary
         */
        public String displayableSummary() {
            return "page=" + pageAddress + " type=" + typeName + " rightSibling=" + rightSiblingAddress + " status="
                    + statusName + " start=" + keyBlockStart + " end=" + keyBlockEnd + " size=" + bufferSize
                    + " alloc=" + alloc + " slack=" + slack + " index=" + poolIndex + " change count=" + timestamp;
        }
    }

    /**
     * Exposes information about a Volume. The {@link #getVolumeInfoArray}
     * returns information about all open Volumes in an array of
     * <code>VolumeInfo</code> elements.
     */
    public static class VolumeInfo extends AcquisitionTimeBase implements Serializable, Comparable<VolumeInfo> {
        public final static long serialVersionUID = -1231320633942497896L;

        int pageSize;
        String path;
        String name;
        long id;
        long createTime;
        long openTime;
        long generation;
        long getCounter;
        long readCounter;
        long writeCounter;
        long lastRead;
        long lastWrite;
        long lastExtension;
        long maximumPage;
        long currentPageCount;
        long maximumPageCount;
        long extensionPageCount;
        long garbageRootPage;
        long fetchCounter;
        long traverseCounter;
        long storeCounter;
        long removeCounter;
        boolean isTemporary;

        VolumeInfo(final Volume vol) {
            super();
            path = vol.getPath();
            id = vol.getId();
            final VolumeStatistics stat = vol.getStatistics();
            final VolumeStructure struc = vol.getStructure();
            final VolumeStorage store = vol.getStorage();
            final VolumeSpecification spec = vol.getSpecification();
            createTime = stat.getCreateTime();
            generation = store.getGeneration();
            getCounter = stat.getGetCounter();
            readCounter = stat.getReadCounter();
            writeCounter = stat.getWriteCounter();
            fetchCounter = stat.getFetchCounter();
            storeCounter = stat.getStoreCounter();
            removeCounter = stat.getRemoveCounter();
            traverseCounter = stat.getTraverseCounter();
            pageSize = struc.getPageSize();
            openTime = stat.getOpenTime();
            lastRead = stat.getLastReadTime();
            lastWrite = stat.getLastWriteTime();
            lastExtension = stat.getLastExtensionTime();
            maximumPage = stat.getNextAvailablePage();
            currentPageCount = store.getExtentedPageCount();
            maximumPageCount = spec.getMaximumPages();
            extensionPageCount = spec.getExtensionPages();
            garbageRootPage = struc.getGarbageRoot();
            isTemporary = vol.isTemporary();
            name = vol.getName();
        }

        @ConstructorProperties({ "pageSize", "path", "name", "id", "createTime", "openTime", "generation",
                "getCounter", "readCounter", "writeCounter", "lastRead", "lastWrite", "lastExtension", "maximumPage",
                "currentPageCount", "maximumPageCount", "extensionPageCount", "garbageRootPage", "fetchCounter",
                "traverseCounter", "storeCounter", "removeCounter", "isTransient" })
        public VolumeInfo(final int pageSize, final String path, final String name, final long id,
                final long createTime, final long openTime, final long generation, final long getCounter,
                final long readCounter, final long writeCounter, final long lastRead, final long lastWrite,
                final long lastExtension, final long maximumPage, final long currentPageCount,
                final long maximumPageCount, final long extensionPageCount, final long garbageRootPage,
                final long fetchCounter, final long traverseCounter, final long storeCounter, final long removeCounter,
                final boolean isTransient) {
            super();
            this.pageSize = pageSize;
            this.path = path;
            this.name = name;
            this.id = id;
            this.createTime = createTime;
            this.openTime = openTime;
            this.generation = generation;
            this.getCounter = getCounter;
            this.readCounter = readCounter;
            this.writeCounter = writeCounter;
            this.lastRead = lastRead;
            this.lastWrite = lastWrite;
            this.lastExtension = lastExtension;
            this.maximumPage = maximumPage;
            this.currentPageCount = currentPageCount;
            this.maximumPageCount = maximumPageCount;
            this.extensionPageCount = extensionPageCount;
            this.garbageRootPage = garbageRootPage;
            this.fetchCounter = fetchCounter;
            this.traverseCounter = traverseCounter;
            this.storeCounter = storeCounter;
            this.removeCounter = removeCounter;
            this.isTemporary = isTransient;
        }

        /**
         * Return the page size for this <code>Volume</code>.
         * 
         * @return the size of each page in this <code>Volume</code>
         */
        public int getPageSize() {
            return pageSize;
        }

        /**
         * Return the full path name.
         * 
         * @return the path name.
         */
        public String getPath() {
            return path;
        }

        /**
         * Return the alias if one was assigned in the system configuration.
         * 
         * @return the alias for this volume, or
         *         <code>null<code> if there is none
         */
        public String getName() {
            return name;
        }

        /**
         * Return the internal identifier value for this <code>Volume</code>
         * 
         * @return the id
         */
        public long getId() {
            return id;
        }

        /**
         * Return the time and date when this <code>Volume</code> was created.
         * 
         * @return The creation date
         */
        public Date getCreateTime() {
            return new Date(createTime);
        }

        /**
         * Return the time and date when this <code>Volume</code> was opened.
         * 
         * @return The date when this <code>Volume</code> was opened.
         */
        public Date getOpenTime() {
            return new Date(openTime);
        }

        /**
         * Return the time and date when the most recent file read occurred.
         * 
         * @return The date when the most recent file read occurred
         */
        public Date getLastReadTime() {
            return new Date(lastRead);
        }

        /**
         * Return the time and date when the most recent file write occurred.
         * 
         * @return The date when the most recent file write occurred
         */
        public Date getLastWriteTime() {
            return new Date(lastWrite);
        }

        /**
         * Return the time and date when the most recent file extension
         * occurred.
         * 
         * @return The date when the most recent file extension occurred
         */
        public Date getLastExtensionTime() {
            return new Date(lastExtension);
        }

        /**
         * Return the generation number for this <code>Volume</code>. The
         * generation number increases as the state of the volume changes.
         * 
         * @return The current generation
         */
        public long getGeneration() {
            return generation;
        }

        /**
         * Return the number of get operations on this <code>Volume</code>. A
         * get operation occurs when a thread attempts to find or update
         * information on the page, regardless of whether the page had
         * previously been copied into the <code>BufferPool</code>.
         * 
         * @return the get counter
         */
        public long getGetCounter() {
            return getCounter;
        }

        /**
         * Return the number of physical read operations performed against pages
         * in this <code>Volume</code>. The read occurs only when Persistit
         * requires the content of a page that has not already been copied into
         * the </code>BufferPool</code> or which has become invalid.
         * 
         * @return the read counter
         */
        public long getReadCounter() {
            return readCounter;
        }

        /**
         * Return the number of physical write operations performed against
         * pages in this <code>Volume</code>.
         * 
         * @return the write counter
         */
        public long getWriteCounter() {
            return writeCounter;
        }

        /**
         * Return the count of {@link Exchange#fetch} operations. These include
         * {@link Exchange#fetchAndStore} and {@link Exchange#fetchAndRemove}
         * operations. This count is maintained within the stored Volume and is
         * not reset when Persistit closes. It is provided to assist in
         * application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getFetchCounter() {
            return fetchCounter;
        }

        /**
         * Return the count of {@link Exchange#traverse} operations. These
         * include {@link Exchange#next}, {@link Exchange#previous} and other
         * derivative operations. This count is maintained within the stored
         * Volume and is not reset when Persistit closes. It is provided to
         * assist in application performance tuning.
         * 
         * @return The count of key traversal operations performed on this in
         *         this Volume.
         */
        public long getTraverseCounter() {
            return traverseCounter;
        }

        /**
         * Return the count of {@link Exchange#store} operations, including
         * {@link Exchange#fetchAndStore} operations. This count is maintained
         * within the stored Volume and is not reset when Persistit closes. It
         * is provided to assist in application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getStoreCounter() {
            return storeCounter;
        }

        /**
         * Return the count of {@link Exchange#remove} operations, including
         * {@link Exchange#fetchAndRemove} operations. This count is maintained
         * within the stored Volume and is not reset when Persistit closes. It
         * is provided to assist in application performance tuning.
         * 
         * @return The count of records fetched from this Volume.
         */
        public long getRemoveCounter() {
            return removeCounter;
        }

        /**
         * Return the current size in bytes of this <code>Volume</code>.
         * 
         * @return current size
         */
        public long getCurrentSize() {
            return currentPageCount * pageSize;
        }

        /**
         * Return the largest page address currently in use within the
         * <code>Volume</code>.
         * 
         * @return the largest page address
         */
        public long getMaximumPageAddress() {
            return maximumPage;
        }

        /**
         * Return the maximum size in bytes to which this <code>Volume</code>
         * may grow.
         * 
         * @return the maximum size
         */
        public long getMaximumSize() {
            return maximumPageCount * pageSize;
        }

        /**
         * Return the size in bytes by which Persistit will extend this
         * <code>Volume</code> when additional file space is required.
         * 
         * @return The extension size
         */
        public long getExtensionSize() {
            return extensionPageCount * pageSize;
        }

        public boolean isTransient() {
            return isTemporary;
        }

        /**
         * Return the name of this Volume
         * 
         * @return The name
         */
        @Override
        public String toString() {
            return name;
        }

        /**
         * Tests whether the supplied object is a <code>VolumeInfo</code> with
         * the same volume name and id as this one.
         * 
         * @return <code>true</code> if equal, otherwise <code>false</code>
         */
        @Override
        public boolean equals(final Object object) {
            return object instanceof VolumeInfo && ((VolumeInfo) object).getId() == id
                    && ((VolumeInfo) object).getName().equals(name);
        }

        @Override
        public int compareTo(final VolumeInfo volumeInfo) {
            return name.compareTo(volumeInfo.name);
        }
    }

    /**
     * Exposes information about a Persistit <code>Tree</code>.
     */
    public static class TreeInfo extends AcquisitionTimeBase implements Serializable {
        public final static long serialVersionUID = -8707513438024673939L;

        String name;
        long rootPageAddress;
        int depth;
        String volumePathName;
        String status;
        String writerThreadName;
        long _fetchCounter;
        long _traverseCounter;
        long _storeCounter;
        long _removeCounter;

        TreeInfo(final Tree tree) {
            super();
            name = tree.getName();
            rootPageAddress = tree.getRootPageAddr();
            depth = tree.getDepth();
            status = tree.getStatusCode();
            volumePathName = tree.getVolume().getPath();
            final Thread thread = tree.getWriterThread();
            writerThreadName = thread == null ? null : thread.getName();
            final TreeStatistics stats = tree.getStatistics();
            this._fetchCounter = stats.getFetchCounter();
            this._traverseCounter = stats.getTraverseCounter();
            this._storeCounter = stats.getStoreCounter();
            this._removeCounter = stats.getRemoveCounter();
        }

        @ConstructorProperties({ "name", "index", "rootPageAddress", "depth", "volumePathName", "status",
                "writerThreadName", "fetchCounter", "traverseCounter", "storeCounter", "removeCounter" })
        public TreeInfo(final String name, final long rootPageAddress, final int depth, final String volumePathName,
                final String status, final String writerThreadName, final long fetchCounter,
                final long traverseCounter, final long storeCounter, final long removeCounter) {
            super();
            this.name = name;
            this.rootPageAddress = rootPageAddress;
            this.depth = depth;
            this.volumePathName = volumePathName;
            this.status = status;
            this.writerThreadName = writerThreadName;
            this._fetchCounter = fetchCounter;
            this._traverseCounter = traverseCounter;
            this._storeCounter = storeCounter;
            this._removeCounter = removeCounter;

        }

        /**
         * Return the name of the <code>Tree</code>
         * 
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * Return the page address of the root page.
         * 
         * @return the root page address
         */
        public long getRootPageAddress() {
            return rootPageAddress;
        }

        /**
         * Return the count of levels in the <code>Tree</code>.
         * 
         * @return the depth
         */
        public int getDepth() {
            return depth;
        }

        /**
         * Return the path name of the volume to which this tree belongs.
         * 
         * @return the path name
         */
        public String getVolumePathName() {
            return volumePathName;
        }

        /**
         * @return the count of {@link Exchange#fetch} operations, including
         *         {@link Exchange#fetchAndStore} and
         *         {@link Exchange#fetchAndRemove} operations.
         */
        public long getFetchCounter() {
            return _fetchCounter;
        }

        /**
         * @return the count of {@link Exchange#traverse} operations including
         *         {@link Exchange#next} and {@link Exchange#previous}.
         */
        public long getTraverseCounter() {
            return _traverseCounter;
        }

        /**
         * @return the count of {@link Exchange#store} operations, including
         *         {@link Exchange#fetchAndStore} operations.
         */
        public long getStoreCounter() {
            return _storeCounter;
        }

        /**
         * @return the count of {@link Exchange#remove} operations, including
         *         {@link Exchange#fetchAndRemove} operations.
         */
        public long getRemoveCounter() {
            return _removeCounter;
        }

        /**
         * Return the status code for this Tree.
         * 
         * @return the status code
         */
        public String getStatus() {
            return status;
        }

        /**
         * Return the name of the <code>Thread</code> that currently holds a
         * writer (exclusive) claim on the <code>Buffer</code> if there is one,
         * otherwise returns <code>null</code>.
         * 
         * @return The thread name, or <code>null</code> if there is none
         */
        public String getWriterThreadName() {
            return writerThreadName;
        }

        /**
         * Return the name of the tree.
         * 
         * @return The name of the tree
         */
        @Override
        public String toString() {
            return name;
        }

        /**
         * Tests whether the supplied object is a <code>TreeInfo</code> with the
         * same index and name is this one.
         * 
         * @return <code>true</code> if equal, otherwise <code>false</code>
         */
        @Override
        public boolean equals(final Object object) {
            return object instanceof TreeInfo && ((TreeInfo) object).getName().equals(name);
        }
    }

    /**
     * Structure that describes the current status of a long-running utility
     * task. See {@link #queryTaskStatus} for further information.
     */
    public static class TaskStatus extends AcquisitionTimeBase implements Serializable {
        public static final long serialVersionUID = 8913876646811877035L;

        long taskId;
        String description;
        String owner;
        int state;
        String stateName;
        long startTime;
        long finishTime;
        long expirationTime;
        String statusSummary;
        String statusDetail;
        String[] newMessages;
        String lastException;

        public TaskStatus() {

        }

        @ConstructorProperties({ "taskId", "description", "owner", "state", "stateName", "startTime", "finishTime",
                "expirationTime", "statusSummary", "statusDetail", "newMessages", "lastException" })
        public TaskStatus(final long taskId, final String description, final String owner, final int state,
                final String stateName, final long startTime, final long finishTime, final long expirationTime,
                final String statusSummary, final String statusDetail, final String[] newMessages,
                final String lastException) {
            super();
            this.taskId = taskId;
            this.description = description;
            this.owner = owner;
            this.state = state;
            this.stateName = stateName;
            this.startTime = startTime;
            this.finishTime = finishTime;
            this.expirationTime = expirationTime;
            this.statusSummary = statusSummary;
            this.statusDetail = statusDetail;
            this.newMessages = newMessages;
            this.lastException = lastException;
        }

        /**
         * Return the unique taskID for this task
         * 
         * @return the unique taskID
         */
        public long getTaskId() {
            return taskId;
        }

        /**
         * Return a description of this task
         * 
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Return the owner (hostname and/or username) of the initiator of this
         * task
         * 
         * @return the owner
         */
        public String getOwner() {
            return owner;
        }

        /**
         * Return the state code for this task. This is one of
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
         * @return The state
         */
        public int getState() {
            return state;
        }

        /**
         * Return the name of the state this task is in. One of "notStarted",
         * "running", "done", "suspended", "failed", "ended", or "expired".
         * 
         * @return A readable name of the task's current state
         */
        public String getStateString() {
            return stateName;
        }

        /**
         * A short (under 40 character) readable String summarizing the current
         * status.
         * 
         * @return A short readable status
         */
        public String getStatusSummary() {
            return statusSummary;
        }

        /**
         * A more detailed description of the current status of the task.
         * 
         * @return Detailed status description
         */
        public String getStatusDetail() {
            return statusDetail;
        }

        /**
         * Messages that have been posted by this task.
         * 
         * @return The messages
         */
        public String[] getMessages() {
            return newMessages;
        }

        /**
         * System time at which this task was started, or zero if never started.
         * 
         * @return The time
         */
        public long getStartTime() {
            return startTime;
        }

        /**
         * System time at which this task ended, whether normal or abnormally,
         * or zero if the task has not ended yet.
         * 
         * @return The time
         */
        public long getFinishTime() {
            return finishTime;
        }

        /**
         * System time at which this task is scheduled to be stopped if it has
         * not finished yet.
         * 
         * @return The time
         */
        public long getExpirationTime() {
            return expirationTime;
        }

        /**
         * The most recent <code>Exception</code> thrown by this task.
         * 
         * @return Most recently thrown <code>Exception</code> by this task
         */
        public String getLastException() {
            return lastException;
        }

        @Override
        public String toString() {
            return toString(false);
        }

        public String toString(final boolean details) {
            if (details) {
                final StringBuilder sb = new StringBuilder(String.format(
                        "%d: %s start=%s finish=%s status=%s exception=%s", taskId, stateName, Util.date(startTime),
                        Util.date(finishTime), statusDetail, lastException));
                for (final String message : newMessages) {
                    sb.append(Util.NEW_LINE);
                    sb.append("  ");
                    sb.append(message);
                }
                return sb.toString();
            } else {
                return taskId + ": " + stateName;
            }
        }

        /**
         * Tests whether the supplied object is a <code>TaskStatus</code> with
         * the same taskId is this one.
         * 
         * @return <code>true</code> if equal, otherwise <code>false</code>
         */
        @Override
        public boolean equals(final Object object) {
            return object instanceof TaskStatus && ((TaskStatus) object).getTaskId() == taskId;
        }
    }

    /**
     * Structure that describes the current state of the journal manager.
     */
    public static class JournalInfo extends AcquisitionTimeBase {
        private static final long serialVersionUID = -6628208913672254686L;

        String currentJournalFile;
        long currentJournalAddress;
        long blockSize;
        int pageMapSize;
        long currentGeneration;
        long baseAddress;
        long lastValidCheckpointTimestamp;
        long lastValidCheckpointSystemTime;
        String lastValidCheckpointJournalFile;
        long lastValidCheckpointJournalAddress;
        String recoveryJournalFile;
        long recoveryJournalAddress;
        long recoveryStatus;
        long journaledPageCount;
        long readPageCount;
        long copiedPageCount;
        long droppedPageCount;
        int recoveredCommittedTransactions;
        int recoveredAppliedTransactions;
        boolean closed;
        boolean copying;
        boolean flushing;
        boolean appendOnly;
        boolean fastCopying;

        public JournalInfo() {

        }

        @ConstructorProperties({ "currentJournalFile", "currentJournalAddress", "blockSize", "pageMapSize",
                "currentGeneration", "baseAddress", "lastValidCheckpointTimestamp", "lastValidCheckpointSystemTime",
                "lastValidCheckpointJournalFile", "lastValidCheckpointJournalAddress", "recoveryJournalFile",
                "recoveryJournalAddress", "recoveryStatus", "journaledPageCount", "readPageCount", "copiedPageCount",
                "droppedPageCount", "recoveredCommittedTransactions", "recoveredAppliedTransactions", "closed",
                "copying", "flushing", "appendOnly", "fastCopying" })
        public JournalInfo(final String currentJournalFile, final long currentJournalAddress, final long blockSize,
                final int pageMapSize, final long currentGeneration, final long baseAddress,
                final long lastValidCheckpointTimestamp, final long lastValidCheckpointSystemTime,
                final String lastValidCheckpointJournalFile, final long lastValidCheckpointJournalAddress,
                final String recoveryJournalFile, final long recoveryJournalAddress, final long recoveryStatus,
                final long journaledPageCount, final long readPageCount, final long copiedPageCount,
                final long droppedPageCount, final int recoveredCommittedTransactions,
                final int recoveredAppliedTransactions, final boolean closed, final boolean copying,
                final boolean flushing, final boolean appendOnly, final boolean fastCopying) {
            super();
            this.currentJournalFile = currentJournalFile;
            this.currentJournalAddress = currentJournalAddress;
            this.blockSize = blockSize;
            this.pageMapSize = pageMapSize;
            this.currentGeneration = currentGeneration;
            this.baseAddress = baseAddress;
            this.lastValidCheckpointTimestamp = lastValidCheckpointTimestamp;
            this.lastValidCheckpointSystemTime = lastValidCheckpointSystemTime;
            this.lastValidCheckpointJournalFile = lastValidCheckpointJournalFile;
            this.lastValidCheckpointJournalAddress = lastValidCheckpointJournalAddress;
            this.recoveryJournalFile = recoveryJournalFile;
            this.recoveryJournalAddress = recoveryJournalAddress;
            this.recoveryStatus = recoveryStatus;
            this.journaledPageCount = journaledPageCount;
            this.readPageCount = readPageCount;
            this.copiedPageCount = copiedPageCount;
            this.droppedPageCount = droppedPageCount;
            this.recoveredCommittedTransactions = recoveredCommittedTransactions;
            this.recoveredAppliedTransactions = recoveredAppliedTransactions;
            this.closed = closed;
            this.copying = copying;
            this.flushing = flushing;
            this.appendOnly = appendOnly;
            this.fastCopying = fastCopying;
        }

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
         * Maximum size of one journal file
         * 
         * @return the Journal file size
         */
        public long getBlockSize() {
            return blockSize;
        }

        /**
         * @return the pageMap size
         */
        public int getPageMapSize() {
            return pageMapSize;
        }

        /**
         * Base address is the journal address of the first record required for
         * recovery.
         * 
         * @return the start generation
         */
        public long getBaseAddress() {
            return baseAddress;
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
         * <table>
         * <tr>
         * <td>Long.MINVALUE</td>
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
         * @return the total number of pages dropped from the journal due the
         *         existence of newer versions
         */
        public long getDroppedPageCount() {
            return droppedPageCount;
        }

        /**
         * @return the total number of pages read from the journal back into
         *         buffer pool.
         */
        public long getReadPageCount() {
            return readPageCount;
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
         * @return <code>true</code> iff the journal has been closed
         */
        public boolean isClosed() {
            return closed;
        }

        /**
         * @return <code>true</code> iff the JOURNALCOPIER task is currently
         *         copying pages to their Volume files.
         */
        public boolean isCopying() {
            return copying;
        }

        /**
         * @return <code>true</code> iff the JOURNALFLUSHER task is currently
         *         forcing file modifications to disk.
         */
        public boolean isFlushing() {
            return flushing;
        }

        /**
         * The appendOnly flag suspends copying of pages to their Volume files.
         * 
         * @return <code>true</code> if the appendOnly flag is set.
         */
        public boolean isAppendOnly() {
            return appendOnly;
        }

        /**
         * @return <code>true</code> if fast page copying is enabled. Fast
         *         copying copies pages as fast as possible without regard to
         *         disk utilization.
         */
        public boolean isFastCopying() {
            return fastCopying;
        }

    }

    /**
     * A structure that holds information about the recovered database. Once
     * recovery is complete, the values in this structure never change.
     * 
     */
    public static class RecoveryInfo extends AcquisitionTimeBase {
        private static final long serialVersionUID = 794617172781091495L;

        String keystoneJournalFile;
        long keystoneJournalAddress;
        long blockSize;
        int pageMapSize;
        long baseAddress;
        long currentAddress;
        long lastValidCheckpointTimestamp;
        long lastValidCheckpointSystemTime;
        String lastValidCheckpointJournalFile;
        long lastValidCheckpointJournalAddress;
        int committedTransactions;
        int uncommittedTransactions;
        int errorCount;
        int appliedTransactions;
        long recoveryStatus;
        long recoveryEndAddress;
        String recoveryException;
        boolean copySuspended;
        boolean copyFast;

        public RecoveryInfo() {

        }

        @ConstructorProperties({ "keystoneJournalFile", "keystoneJournalAddress", "blockSize", "pageMapSize",
                "baseAddress", "currentAddress", "lastValidCheckpointTimestamp", "lastValidCheckpointSystemTime",
                "lastValidCheckpointJournalFile", "lastValidCheckpointJournalAddress", "committedTransactions",
                "uncommittedTransactions", "errorCount", "appliedTransactions", "recoveryStatus", "recoveryEndAddress",
                "recoveryException", "copySuspended", "copyFast" })
        public RecoveryInfo(final String keystoneJournalFile, final long keystoneJournalAddress, final long blockSize,
                final int pageMapSize, final long baseAddress, final long currentAddress,
                final long lastValidCheckpointTimestamp, final long lastValidCheckpointSystemTime,
                final String lastValidCheckpointJournalFile, final long lastValidCheckpointJournalAddress,
                final int committedTransactions, final int uncommittedTransactions, final int errorCount,
                final int appliedTransactions, final long recoveryStatus, final long recoveryEndAddress,
                final String recoveryException, final boolean copySuspended, final boolean copyFast) {
            super();
            this.keystoneJournalFile = keystoneJournalFile;
            this.keystoneJournalAddress = keystoneJournalAddress;
            this.blockSize = blockSize;
            this.pageMapSize = pageMapSize;
            this.baseAddress = baseAddress;
            this.currentAddress = currentAddress;
            this.lastValidCheckpointTimestamp = lastValidCheckpointTimestamp;
            this.lastValidCheckpointSystemTime = lastValidCheckpointSystemTime;
            this.lastValidCheckpointJournalFile = lastValidCheckpointJournalFile;
            this.lastValidCheckpointJournalAddress = lastValidCheckpointJournalAddress;
            this.committedTransactions = committedTransactions;
            this.uncommittedTransactions = uncommittedTransactions;
            this.errorCount = errorCount;
            this.appliedTransactions = appliedTransactions;
            this.recoveryStatus = recoveryStatus;
            this.recoveryEndAddress = recoveryEndAddress;
            this.recoveryException = recoveryException;
            this.copySuspended = copySuspended;
            this.copyFast = copyFast;
        }

        /**
         * @return the keystone Journal file name. This is the last-written
         *         valid journal file in the journal.
         */
        public String getKeystoneJournalFile() {
            return keystoneJournalFile;
        }

        /**
         * @return the keystone Journal file address
         */
        public long getKeystoneJournalAddress() {
            return keystoneJournalAddress;
        }

        /**
         * @return the max Journal file size
         */
        public long getBlockSize() {
            return blockSize;
        }

        /**
         * Number of page entries in the recovered page map
         * 
         * @return the pageMap size
         */
        public int getPageMapSize() {
            return pageMapSize;
        }

        /**
         * Address of first record in the journal required for recovery.
         * 
         * @return the start generation
         */
        public long getBaseAddress() {
            return baseAddress;
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
         * Address within journal file from which records currently are being
         * scanned. After recovery is complete, the address after the last
         * recovered record.
         * 
         * @return the current recovery file address.
         */
        public long getCurrentAddress() {
            return currentAddress;
        }

        /**
         * Return one of the following values:
         * <table>
         * <tr>
         * <td>Long.MINVALUE</td>
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
         * @return the number of committed Transactions found in the journal
         */
        public int getCommittedTransactions() {
            return committedTransactions;
        }

        /**
         * @return the number of uncommitted Transactions found in the journal
         */
        public int getUncommittedTransactions() {
            return uncommittedTransactions;
        }

        /**
         * @return the number of recovered committed Transactions that have been
         *         applied
         */
        public int getAppliedTransactions() {
            return appliedTransactions;
        }

        /**
         * @return the number of errors encountered while applying committed
         *         Transactions during recovery
         */
        public int getErrorCount() {
            return errorCount;
        }

        /**
         * @return journal address at which recovery processing stopped
         */
        public long getRecoveryEndAddress() {
            return recoveryEndAddress;
        }

        /**
         * @return Exception that caused recovery to end or the empty string for
         *         recovery after a clean shutdown.
         */
        public String getRecoveryEndedException() {
            return recoveryException;
        }
    }

    /**
     * A structure that holds counts of commit, rollbacks and rollbacks since
     * the last commit. Roll-backs since last commit is computed by adding the
     * rollbacks since last commit of each {@link Transaction} whose session
     * owned Thread that is still alive. Therefore the statistics include
     * <code>Transaction</code> contexts that may not have executed a
     * transaction recently.
     * 
     */
    public static class TransactionInfo extends AcquisitionTimeBase {

        private static final long serialVersionUID = -8355613679126582750L;

        long commitCount;
        long rollbackCount;
        long rollbackSinceCommitCount;

        public TransactionInfo() {

        }

        @ConstructorProperties({ "commitCount", "rollbackCount", "rollbackSinceCommitCount" })
        public TransactionInfo(final long commitCount, final long rollbackCount, final long rollbackSinceCommitCount) {
            this.commitCount = commitCount;
            this.rollbackCount = rollbackCount;
            this.rollbackSinceCommitCount = rollbackSinceCommitCount;
        }

        /**
         * Return the aggregate number of transactions committed since Persistit
         * was initialized
         * 
         * @return total number of transactions committed
         * @throws RemoteException
         */
        public long getCommitCount() {
            return commitCount;
        }

        /**
         * Return the aggregate number of transaction rollback events since
         * Persistit was initialized
         * 
         * @return total number of transactions rolled back
         * @throws RemoteException
         */
        public long getRollbackCount() {
            return rollbackCount;
        }

        /**
         * Return the aggregate number of transaction rollback events since the
         * last commit
         * 
         * @return number of transactions rolled back since the last commit
         * @throws RemoteException
         */
        public long getRollbackSinceCommitCount() {
            return rollbackSinceCommitCount;
        }

    }

    /**
     * A subclass of <code>RemoteException</code> that wraps a
     * <code>Throwable</code> so that it can be returned to the remote client.
     */
    public static class WrappedRemoteException extends RemoteException {

        public static final long serialVersionUID = 3297945951883397630L;

        Throwable cause;

        public WrappedRemoteException(final Throwable cause) {
            this.cause = cause;
        }

        @Override
        public Throwable getCause() {
            return cause;
        }
    }

}
