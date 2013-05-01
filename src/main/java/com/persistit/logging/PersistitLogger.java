/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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
