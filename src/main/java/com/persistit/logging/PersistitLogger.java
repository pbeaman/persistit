/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.logging;

/**
 * Simple interface for attaching an external logger.
 * 
 * @author peter
 * 
 */
public interface PersistitLogger {

    /**
     * Write the message to the log at the specified level. Configuration will
     * ensure that this method is called only when the level is indeed loggable.
     * 
     * @param level
     *            The level of the log event
     * @param message
     *            A string-valued description of the event
     */
    void log(final PersistitLevel level, final String message);

    /**
     * Test whether a message at the specified level should be written to the
     * log. This method is called during configuration, not once per log
     * message.
     * 
     * @param level
     *            The level of the log event
     * @return whether a message should be logged at the specified level
     */
    boolean isLoggable(final PersistitLevel level);

    /**
     * Called when Persistit starts using the log.
     * 
     * @throws Exception
     */
    void open() throws Exception;

    /**
     * Called when Persistit stops using the log.
     * 
     * @throws Exception
     */
    void close() throws Exception;

    /**
     * Flush pending ouput
     */
    void flush();
}
