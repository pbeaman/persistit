/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
