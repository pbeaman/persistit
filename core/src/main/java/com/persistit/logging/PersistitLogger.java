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
package com.persistit.logging;

/**
 * Simple interface for attaching an external logger.
 * 
 * @author peter
 * 
 */
public interface PersistitLogger {

    /**
     * Emit the message to the log at the specified level. Configuration
     * will ensure that this method is called only when the level is
     * indeed loggable.
     * @param level
     * @param message
     */
    void log(final PersistitLevel level, final String message);

    /**
     * Test whether a message at the specified level should be written to
     * the log.  This method is called during configuration, not once per
     * log message.
     * @param level
     * @return
     */
    boolean isLoggable(final PersistitLevel level);

    /**
     * Called when Persistit starts using the log.
     * @throws Exception
     */
    public void open() throws Exception;

    /**
     * Called when Persistit stos using the log.
     * @throws Exception
     */
    public void close() throws Exception;
}
