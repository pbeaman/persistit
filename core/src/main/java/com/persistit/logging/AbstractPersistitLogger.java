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
 * Created on Feb 19, 2004
 */
package com.persistit.logging;

import java.util.Stack;

/**
 * <p>
 * Abstract superclass of logging implementations for Persistit events.
 * Persistit uses an internal logging mechanism that is designed to be easily
 * adapted to products in which it is embedded. By default, Persistit uses a
 * {@link DefaultPersistitLogger} to format and write log messages to a file.
 * <tt>DefaultPersistitLogger</tt> also writes high-level log messages to the
 * system output device.
 * </p>
 * <p>
 * Two concrete subclasses make logging to the JDK 1.4 logging mechanism or to
 * Jakarta Log4J particularly easy. Simply add one of these two lines of code
 * before initializing Persistit to redirect log output either to either a
 * <tt>java.util.logging.Logger</tt> (available in JDK 1.4 and above) or to an
 * <tt>org.apache.log4j.Logger</tt>. <code><pre>
 *      Persistit.setPersistitLogger(new JDK14LoggingAdapter(jdk14Logger));
 * </pre></code> or <code><pre>
 *      Persistit.setPersistitLogger(new Log4JAdapter(log4jLogger));
 * </pre></code>
 * </p>
 * <p>
 * To change the behavior of logging in other ways, subclass this abstract class
 * and then register your custom logger by invoking
 * {@link com.persistit.Persistit#setPersistitLogger}. Typically only the
 * {@link #log(LogTemplate, String)} and {@link #isLoggable} methods need to be
 * overridden.
 * </p>
 * <p>
 * This class defines several event severity levels, ranging from FINEST to
 * ALWAYS. The names of these levels correspond to the JDK 1.4 logging API, but
 * for backward compatibility do not rely on the presence of the
 * <tt>java.util.logging.Level</tt> class. The adapters described above map the
 * internal Persistit levels to appropriate values for JDK 1.4 logging and
 * Log4J, respectively.
 * </p>
 * 
 * @version 1.0
 */
public abstract class AbstractPersistitLogger {

    public final static int FINEST = 1;
    public final static int FINER = 2;
    public final static int FINE = 3;
    public final static int INFO = 4;
    public final static int WARNING = 5;
    public final static int SEVERE = 6;
    public final static int ALWAYS = 9;

    public final static LogTemplate GENERAL_LOG_TEMPLATE = new LogTemplate(
            ALWAYS, -1, "Persistit: {0}");

    public final static String[] LEVEL_NAMES = { "?", // 0
            "FINEST", // 1
            "FINER", // 2
            "FINE", // 3
            "INFO", // 4
            "WARNING", // 5
            "SEVERE", // 6
            "?", // 7
            "?", // 8
            "ALWAYS" // 9
    };

    private final static Stack _sbStack = new Stack();

    private boolean _logThreadName = true;
    private boolean _logLevelName = false;

    /**
     * Unconditionally log a message.
     * 
     * @param message
     *            The message
     */
    public void log(String message) {
        log(GENERAL_LOG_TEMPLATE, message);
    }

    /**
     * Writes a formated message to the log. Override this method to write the
     * message to the enclosing system's log. Prior to calling this method
     * Persistit localizes and formats the message String using its own resource
     * bundle (LogBase_xx.properties). The implementation may refer to
     * information in the LogTempate to permit filtering or special handling.
     * 
     * @param logTemplate
     *            The <code>LogTemplate</code> from which this messge was
     *            formated
     * @param message
     *            The resulting message String, ready to be written to a log.
     */
    public abstract void log(LogTemplate logTemplate, String message);

    /**
     * Controls whether each formated log message will include the name of the
     * Thread in which the logged event occurred.
     * 
     * @param b
     *            <i>true</i> (default) to enable.
     */
    public void setLogThreadName(boolean b) {
        _logThreadName = b;
    }

    /**
     * Indicates whether each formated log message will include the name of the
     * Thread in which the logged event occurred.
     * 
     * @return <i>true</i> if enabled.
     */
    public boolean isLogThreadName() {
        return _logThreadName;
    }

    /**
     * Controls whether each formated log message will include the severity
     * level of the event.
     * 
     * @param b
     *            <i>true</i> (default) to enable.
     */
    public void setLogLevelName(boolean b) {
        _logLevelName = b;
    }

    /**
     * Indicates whether each formated log message will include the severity
     * level of the event.
     * 
     * @return <i>true</i> if enabled.
     */
    public boolean isLogLevelName() {
        return _logLevelName;
    }

    /**
     * Determines whether a message will be logged. This method is called before
     * the log message is actually formated. The default implementation reflects
     * the internal enabled state of the LogTemplate. Override this for
     * additional control.
     * 
     * @param lt
     *            The LogTemplate for a proposed log event.
     * @return <i>true</i> if that even should be logged.
     */
    public boolean isLoggable(LogTemplate lt) {
        return lt.isEnabled();
    }

    /**
     * Logs an event. The event is defined by one of a predefined set of
     * {@link LogTemplate}s. Up to ten Object-valued information items may be
     * supplied from the context in which the event was recognized. The
     * <code>LogTemplate</code> supplies a localized String template. This
     * template may contain substitution variables {0}, {1}, ... {9} which are
     * to be replaced by the parameters <code>p0</code>, <code>p1</code>, ...
     * <code>p9</code>.
     * <p>
     * 
     * This method may be overridden to provide greater control over logging
     * behavior, but the default implementation is typically adequate. This
     * method is invoked by time-sensitive Threads, so the implementation should
     * be fast and non-blocking. Note that if you override this method, the
     * {@link #log(LogTemplate, String)} method will be called only if your
     * implementation does so.
     * 
     * @param logTemplate
     *            Definition of the logged event
     * @param p0
     *            Value to be substituted for any occurrence of {0} in the
     *            template
     * @param p1
     *            Value to be substituted for any occurrence of {1} in the
     *            template
     * @param p2
     *            Value to be substituted for any occurrence of {2} in the
     *            template
     * @param p3
     *            Value to be substituted for any occurrence of {3} in the
     *            template
     * @param p4
     *            Value to be substituted for any occurrence of {4} in the
     *            template
     * @param p5
     *            Value to be substituted for any occurrence of {5} in the
     *            template
     * @param p6
     *            Value to be substituted for any occurrence of {6} in the
     *            template
     * @param p7
     *            Value to be substituted for any occurrence of {7} in the
     *            template
     * @param p8
     *            Value to be substituted for any occurrence of {8} in the
     *            template
     * @param p9
     *            Value to be substituted for any occurrence of {9} in the
     *            template
     */
    public void log(LogTemplate logTemplate, Object... args) {
        StringBuilder sb;
        synchronized (_sbStack) {
            if (_sbStack.isEmpty())
                sb = new StringBuilder();
            else
                sb = (StringBuilder) _sbStack.pop();
        }

        sb.setLength(0);

        if (_logThreadName) {
            sb.append("[");
            sb.append(Thread.currentThread().getName());
            sb.append("] ");
        }

        if (_logLevelName) {
            sb.append(LEVEL_NAMES[logTemplate._level]);
            sb.append(' ');
        }

        String template = logTemplate.getTemplate();
        int q = 0;
        for (int p = 0; p != -1; p = template.indexOf('{', p)) {
            p++; // No matter what, advance the search index
            if (p < template.length() - 1 && template.charAt(p + 1) == '}') {
                int c = template.charAt(p);
                if (c >= '0' && c < '9') {
                    sb.append(template.substring(q, p - 1));
                    p += 2;
                    q = p;
                    if (c - '0' < args.length) {
                        sb.append(args[c - '0']);
                    } else {
                        sb.append("???");
                    }
                }
            }
        }
        if (q < template.length())
            sb.append(template.substring(q));

        log(logTemplate, sb.toString());

        synchronized (_sbStack) {
            _sbStack.push(sb);
        }

    }

    /**
     * Indicates whether log messages can be written.
     * 
     * @return <i>true</i> if there is a log open and ready to receive events.
     */
    public boolean isOpen() {
        return true;
    }

    /**
     * Starts the logging system. If the log is already open, this method throws
     * an {@link IllegalStateException}. An overriding implementation may throw
     * any type of <code>Exception</code>.
     * 
     * @throws Exception
     */
    public void open() throws Exception {
    }

    /**
     * Closes Persistit's log session. The default implementation closes the log
     * file. If Persistit's log is embedded in a host product, this method
     * should typically do nothing.
     * 
     */
    public void close() {
        flush();
    }

    /**
     * Writes pending messages to the underlying log representation. The default
     * implementation flushes pending buffer output to the log file. Persistit
     * calls this method periodically to ensure that logged records have been
     * fully written to the log output device.
     * 
     */
    public void flush() {
    }

}
