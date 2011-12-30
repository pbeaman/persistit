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

import java.rmi.RemoteException;

/**
 * Interface for a service object that exposes information about the Persistit
 * environment. With this public API, embedding applications can query
 * performance metrics and resources within Persistit that are not exposed by
 * the normal access methods. For example, this class provides methods that
 * enumerate the volumes currently mounted, the size and effectiveness of buffer
 * pool components, and many other items that may be useful in tuning and
 * managing Persistit.
 * 
 * @version 1.0
 */
public interface ManagementMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit";

    /**
     * Indicate whether Persistit is currently in the initialized state.
     * 
     * @return The state
     */
    public boolean isInitialized() throws RemoteException;

    /**
     * Return the version name of the current Persistit instance.
     * 
     * @return the version name
     */
    public String getVersion() throws RemoteException;

    /**
     * Return the copyright notice for the current Persistit instance.
     * 
     * @return the copyright notice
     */
    public String getCopyright() throws RemoteException;

    /**
     * Return the system time at which Persistit was initialized.
     * 
     * @return start time, in milliseconds since January 1, 1970 00:00:00 GMT.
     */
    public long getStartTime() throws RemoteException;

    /**
     * Return the elapsed time since startup in milliseconds
     * 
     * @return elapsed time in milliseconds
     */
    public long getElapsedTime() throws RemoteException;

    /**
     * Return the port on which a local RMI server has been registered, or -1 if
     * not registered.
     * 
     * @return the port
     * @throws RemoteException
     */
    public int getRmiPort() throws RemoteException;

    /**
     * Return the aggregate number of transactions committed since Persistit was
     * initialized
     * 
     * @return total number of transactions committed
     * @throws RemoteException
     */
    public long getCommittedTransactionCount() throws RemoteException;

    /**
     * Return the aggregate number of transaction rollback events since
     * Persistit was initialized
     * 
     * @return total number of transactions rolled back
     * @throws RemoteException
     */
    public long getRollbackCount() throws RemoteException;

    /**
     * @param max Maximum number of transactions to report on.
     * @return Report on the <code>max</code> longest-running transactions.
     * @throws RemoteException
     */
    public String transactionReport(int max) throws RemoteException;

    /**
     * Indicate whether Persistit will suspend its shutdown activities on
     * invocation of {@link com.persistit.Persistit#close}. This flag is
     * intended for use by management tools that need to keep Persistit open
     * even when the application has requested it to close so that the final
     * state of the Persistit environment can be examined.
     * 
     * @return <code>true</code> if Persistit will wait when attempting to
     *         close; <code>false</code> if the <code>close</code> operation
     *         will not be suspended.
     */
    public boolean isShutdownSuspended() throws RemoteException;

    /**
     * Control whether Persistit will suspend its shutdown activities on
     * invocation of {@link com.persistit.Persistit#close}. This flag is
     * intended for use by management tools that need to keep Persistit open
     * even when the application has requested it to close so that the final
     * state of the Persistit environment can be examined.
     * 
     * @param suspended
     *            <code>true</code> to specify that Persistit will wait when
     *            attempting to close; otherwise <code>false</code>.
     */
    public void setShutdownSuspended(boolean suspended) throws RemoteException;

    /**
     * Indicate whether Persistit is suspending all update operations. When
     * enabled, Persistit will indefinitely delay each Thread that attempts to
     * perform an update operation.
     * 
     * @return <code>true</code> if Persistit will suspend any attempt to update
     *         a <code>Volume</code>; otherwise <code>false</code>.
     * @throws RemoteException
     */
    public boolean isUpdateSuspended() throws RemoteException;

    /**
     * Control whether Persistit will suspend all update operations. When
     * enabled, Persistit will delay each Thread that attempts to perform an
     * update operation indefinitely.
     * 
     * @param suspended
     * @throws RemoteException
     */
    public void setUpdateSuspended(boolean suspended) throws RemoteException;

    /**
     * Flush and sync all dirty data in Persistit by invoking
     * {@link Persistit#flush} and {@link Persistit#sync}.
     * 
     * @throws RemoteException
     */
    public void flushAndSync() throws RemoteException;

    /**
     * Use a simple command-line processor to invoke a task. Allows task
     * invocation to be scripted through a command-line JMX client. This method
     * runs the Task, which in some cases may take a long time. In contrast, the
     * {@link #launch(String)} method initiates the task in a new thread and
     * returns immediately.
     * 
     * @param arg
     * @return The final status of the Task, as a String
     * @throws RemoteException
     */
    public String execute(final String commandLine) throws RemoteException;

    /**
     * Use a simple command-line processor to start a task. Allows task
     * invocation to be scripted through a command-line JMX client. This method
     * starts the task in a new thread and returns immediately. In contrast, the
     * {@link #execute(String)} method completes the task before returning.
     * 
     * @param arg
     * @return The taskId, as a String
     * @throws RemoteException
     */
    public String launch(final String commandLine) throws RemoteException;
}
