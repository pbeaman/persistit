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

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Date;

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
public interface ManagementMXBean {
    
    public final static String MXBEAN_NAME = "com.persistit:type=Persistit";

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
     * Attempts to flush and sync all dirty data in Persistit by invoking
     * {@link Persistit#flush} and {@link Persistit#sync}.
     * 
     * @return <tt>true</tt> if the attempt to close Persistit was successful;
     *         otherwise <tt>false</tt>
     * @throws RemoteException
     */
    public boolean flushAndSync() throws RemoteException;

}
