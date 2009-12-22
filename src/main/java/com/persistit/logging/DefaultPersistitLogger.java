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
 * Created on Mar 1, 2004
 */
package com.persistit.logging;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * The default implementation of logging for Persistit.  Writes log
 * messages to a file.
 *
 * @version 1.0
 */
public class DefaultPersistitLogger
extends AbstractPersistitLogger
{
    private static PrintWriter _logWriter;
    
    private static final String DEFAULT_LOG_FILE_NAME = "./persistit.log";
    private static final long FLUSH_DELAY_INTERVAL = 5000;
    
    private String _logFileName;
    private DefaultPersistitLogFlusher _logFlusher;
    
    /**
     * Background thread that periodically flushes the log file buffers
     * so that we actually have log information in the event of a failure.
     */
    private class DefaultPersistitLogFlusher
    extends Thread
    {
        boolean _stop;
        
        DefaultPersistitLogFlusher()
        {
            setDaemon(true);
            setName("LogFlusher");
        }
        
        public void run()
        {
            while (!_stop)
            {
                try
                {
                    Thread.sleep(FLUSH_DELAY_INTERVAL);
                }
                catch (InterruptedException ie)
                {
                }
                flush();
            }
        }
    }
    
    /**
     * Constructs a logger that logs messages to a file named "persistit.log" 
     * in the current working directory.
     */
    public DefaultPersistitLogger()
    {
        this(null);
    }
    
    /**
     * Constructs a logger that logs messages to a file.
     * @param   fileName    Log file path name
     */
    public DefaultPersistitLogger(String fileName)
    {
        if (fileName == null) fileName = DEFAULT_LOG_FILE_NAME;
        _logFileName = fileName;
    }

    /**
     * Writes a message to the log file and also displays high-significance
     * messages to <tt>System.err</tt>.
     * @param logTemplate   Ignored in this implementation
     * @param message       The message to write
     */
    public void log(LogTemplate logTemplate, String message)
    {
        if (logTemplate._level > INFO ||
            (logTemplate._level > FINE && _logWriter == null))
        {
            System.err.println(message);
        }
        if (_logWriter != null)
        {
            _logWriter.println(message);
        }

    }
    
    /**
     * Detects whether the log file is open.
     * @return  <tt>true</tt> if the log file is open 
     */
    public boolean isOpen()
    {
        return _logWriter != null;
    }
    
    /**
     * Prepares this logger to received messages.  This implementation,
     * opens the file for writing.
     */
    public void open()
    throws Exception
    {
        if (isOpen()) throw new IllegalStateException("Log already open");
        
        _logWriter = new PrintWriter(
            new BufferedWriter(
                new FileWriter(_logFileName)));
        
        _logFlusher = new DefaultPersistitLogFlusher();
        _logFlusher.start();
    }
    
    /**
     * Closes the log file.
     */
    public void close()
    {
        if (_logWriter != null)
        {
            _logWriter.close();
            _logWriter = null;
            _logFlusher._stop = true;
            _logFlusher = null;
        }
    }
    
    /**
     * Writes pending I/O to the log file.  Because this implementation
     * uses a <tt>BufferedWriter</tt>, log messages may be held in memory
     * rather than being written to disk.  Since the most interesting log
     * messages in the event of a failure are often the last messages, it is
     * helpful for these messages to be flushed to disk on a regular basis.
     * This method is invoked at various times within the execution of
     * Persistit to increase the likelihood that the log file will be useful
     * in case the application exits abruptly.
     */
    public void flush()
    {
        if (_logWriter != null)
        {
            _logWriter.flush();
        }
    }
    

}
