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

package com.persistit.logging;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * The default implementation of logging for Persistit. Writes log messages to a
 * file and/or System.err. Many applications will uses one of the logging
 * adapter classes instead of this class to connect Persistit's log output to an
 * existing infrastructure.
 * 
 * @see ApacheCommonsLogAdapter
 * @see JDK14LoggingAdapter
 * @see Log4JAdapter
 * @see Slf4jAdapter
 * 
 * @version 1.0
 */
public class DefaultPersistitLogger implements PersistitLogger {

    private volatile PrintWriter _logWriter;
    private final String _logFileName;
    private PersistitLevel _level = PersistitLevel.INFO;

    /**
     * Construct a logger that logs messages at WARNING level or above to the
     * system console.
     */
    public DefaultPersistitLogger() {
        this(null);
    }

    /**
     * Construct a logger that logs messages to a file. If the supplied path
     * name is <code>null</code> then log only WARNING and higher messages to
     * the system console.
     * 
     * @param fileName
     *            Log file path name
     */
    public DefaultPersistitLogger(final String fileName) {
        _logFileName = fileName;
    }

    public void setLevel(final String levelName) {
        try {
            setLevel(PersistitLevel.valueOf(levelName));
        } catch (final EnumConstantNotPresentException e) {
            log(PersistitLevel.WARNING, "No such log level " + levelName);
            setLevel(PersistitLevel.INFO);
        }
    }

    public void setLevel(final PersistitLevel level) {
        _level = level;
    }

    public PersistitLevel getLevel() {
        return _level;
    }

    /**
     * Writes a message to the log file and also displays high-significance
     * messages to <code>System.err</code>.
     * 
     * @param level
     *            PersistitLevel at which this message should be logged.
     *            (Ignored in this implementation.)
     * @param message
     *            The message to write
     */
    @Override
    public void log(final PersistitLevel level, final String message) {
        final PrintWriter logWriter = _logWriter;
        if (logWriter == null && level.compareTo(PersistitLevel.WARNING) >= 0
                || level.compareTo(PersistitLevel.WARNING) >= 0) {
            System.err.println(message);
        }
        if (logWriter != null) {
            logWriter.println(message);
        }

    }

    @Override
    public boolean isLoggable(final PersistitLevel level) {
        return level.compareTo(_level) >= 0;
    }

    /**
     * Prepares this logger to received messages. This implementation, opens the
     * file for writing.
     */
    @Override
    public void open() throws Exception {
        if (_logWriter != null) {
            throw new IllegalStateException("Log already open");
        }
        if (_logFileName != null) {
            _logWriter = new PrintWriter(new BufferedWriter(new FileWriter(_logFileName)));
        }
    }

    /**
     * Closes the log file.
     * 
     * @throws InterruptedException
     */
    @Override
    public void close() throws InterruptedException {
        flush();
        final PrintWriter logWriter = _logWriter;
        if (logWriter != null) {
            logWriter.close();
        }
        _logWriter = null;
    }

    /**
     * Writes pending I/O to the log file. Because this implementation uses a
     * <code>BufferedWriter</code>, log messages may be held in memory rather
     * than being written to disk. Since the most interesting log messages in
     * the event of a failure are often the last messages, it is helpful for
     * these messages to be flushed to disk on a regular basis. This method is
     * invoked at various times within the execution of Persistit to increase
     * the likelihood that the log file will be useful in case the application
     * exits abruptly.
     */
    @Override
    public void flush() {
        final PrintWriter logWriter = _logWriter;
        if (logWriter != null) {
            logWriter.flush();
        }
    }

}
