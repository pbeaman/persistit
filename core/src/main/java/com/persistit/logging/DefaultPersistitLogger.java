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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * The default implementation of logging for Persistit. Writes log messages to a
 * file and/or System.err.
 * 
 * @version 1.0
 */
public class DefaultPersistitLogger implements PersistitLogger {

    private final static String DEFAULT_LOG_FILE_NAME = "./persistit.log";
    private final static long FLUSH_DELAY_INTERVAL = 5000;

    private PrintWriter _logWriter;
    private String _logFileName;
    private DefaultPersistitLogFlusher _logFlusher;
    private PersistitLevel _level = PersistitLevel.INFO;

    /**
     * Background thread that periodically flushes the log file buffers so that
     * we actually have log information in the event of a failure.
     */
    private class DefaultPersistitLogFlusher extends Thread {
        boolean _stop;

        DefaultPersistitLogFlusher() {
            setDaemon(true);
            setName("LogFlusher");
        }

        @Override
        public void run() {
            while (!_stop) {
                try {
                    Thread.sleep(FLUSH_DELAY_INTERVAL);
                } catch (InterruptedException ie) {
                    break;
                }
                flush();
            }
        }
    }

    /**
     * Constructs a logger that logs messages to a file named "persistit.log" in
     * the current working directory.
     */
    public DefaultPersistitLogger() {
        this(null);
    }

    public void setLevel(final String levelName) {
        try {
            setLevel(PersistitLevel.valueOf(levelName));
        } catch (EnumConstantNotPresentException e) {
            log(PersistitLevel.WARNING, "No such log level " + levelName);
            setLevel(PersistitLevel.INFO);
        }
    }

    public void setLevel(PersistitLevel level) {
        _level = level;
    }

    public PersistitLevel getLevel() {
        return _level;
    }

    /**
     * Constructs a logger that logs messages to a file.
     * 
     * @param fileName
     *            Log file path name
     */
    public DefaultPersistitLogger(String fileName) {
        if (fileName == null)
            fileName = DEFAULT_LOG_FILE_NAME;
        _logFileName = fileName;
    }

    /**
     * Writes a message to the log file and also displays high-significance
     * messages to <code>System.err</code>.
     * 
     * @param logTemplate
     *            Ignored in this implementation
     * @param message
     *            The message to write
     */
    @Override
    public void log(PersistitLevel level, String message) {
        if (_logWriter == null && level.compareTo(PersistitLevel.DEBUG) >= 0
                || level.compareTo(PersistitLevel.WARNING) >= 0) {
            System.err.println(message);
        }
        if (_logWriter != null) {
            _logWriter.println(message);
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
    public void open() throws Exception {
        if (_logWriter != null) {
            throw new IllegalStateException("Log already open");
        }
        _logWriter = new PrintWriter(new BufferedWriter(new FileWriter(_logFileName)));
        _logFlusher = new DefaultPersistitLogFlusher();
        _logFlusher.start();
    }

    /**
     * Closes the log file.
     * @throws InterruptedException 
     */
    public void close() throws InterruptedException {
        if (_logWriter != null) {
            _logWriter.close();
            _logWriter = null;
        }

        if (_logFlusher != null) {
            _logFlusher._stop = true;
            _logFlusher.interrupt();
            _logFlusher.join();
            _logFlusher = null;
        }
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
    public void flush() {
        if (_logWriter != null) {
            _logWriter.flush();
        }
    }

}
