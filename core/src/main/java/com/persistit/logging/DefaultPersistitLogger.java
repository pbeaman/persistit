/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
    private String _logFileName;
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
    public DefaultPersistitLogger(String fileName) {
        _logFileName = fileName;
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
    public void log(PersistitLevel level, String message) {
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
    public void flush() {
        final PrintWriter logWriter = _logWriter;
        if (logWriter != null) {
            logWriter.flush();
        }
    }

}
