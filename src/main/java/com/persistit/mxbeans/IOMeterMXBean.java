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

package com.persistit.mxbeans;

import java.io.IOException;

import javax.management.MXBean;

/**
 * <p>
 * Manages statistics about various types of physical I/O operations. These are
 * measures of I/O calls to the the file system and therefore only approximately
 * represent the true volume of physical I/O operations.
 * </p>
 * 
 * @author peter
 * 
 */
@MXBean
public interface IOMeterMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=IOMeter";

    /**
     * Abbreviated names for types of I/O operations measured by this bean:
     * <dl>
     * <dt>RV</dt>
     * <dd>Read page from Volume</dd>
     * <dt>RJ</dt>
     * <dd>Read page from Journal</dd>
     * <dt>CC</dt>
     * <dd>Copy page from journal to volume</dd>
     * <dt>WJ</dt>
     * <dd>Write page from Journal</dd>
     * <dt>TJ</dt>
     * <dd>Write Transaction to Journal</dd>
     * <dt>XX</dt>
     * <dd>Other</dd>
     * <dt>EV</dt>D
     * <dd>Evict page from pool</dd>
     * <dt>FJ</dt>
     * <dd>Flush journal</dd>
     * <dt>GP</dt>
     * <dd>Get Page</dd>
     * </dl>
     * 
     */
    public final static String[] OPERATION_NAMES = { "Unknown", "Read page from Volume", "Read page from Journal",
            "Copy page from journal", "Copy page to volume", "Write page from Journal", "Write Transaction to Journal", "Other",
            "Evict page from pool", "Flush journal", "Get page" };

    public final static String[] OPERATIONS = { "??", "RV", "RJ", "CJ", "CV", "WJ", "TJ", "XX", "EV", "FJ", "GP" };

    public final static String[] SUMMARY_ITEMS = { "CJ", "CV", "RV", "RJ", "WJ", "EV", "FJ" };

    /**
     * @return the quiescentIOthreshold
     */
    @Description("Disk I/O scheduling parameter in bytes per second specifying threshold "
            + "between \"quiescent\" and \"busy\" states")
    public long getQuiescentIOthreshold();

    /**
     * @param quiescentIO
     *            the quiescentIOthreshold to set
     */
    @Description("Disk I/O scheduling parameter in bytes per second specifying threshold "
            + "between \"quiescent\" and \"busy\" states")
    public void setQuiescentIOthreshold(long quiescentIO);

    /**
     * @return the ioRate
     */
    @Description("Approximate I/O rate in bytes per second")
    public long getIoRate();

    /**
     * Set the path for file into which IO events should be logged, or
     * <code>null</code> to disable IO logging. Use care when enabling an I/O
     * log because every I/O operation performed by Persistit results in a
     * fixed-length record being added to the file. The resulting log file can
     * become extremely large.
     * 
     * @param toFile
     * @throws IOException
     */
    @Description("Path for diagnostic I/O log file - normally null")
    public void setLogFile(final String toFile) throws IOException;

    /**
     * @return Path for file into which IO events should be logged, or
     *         <code>null</code> if IO logging is disabled.
     */
    @Description("Path for diagnostic I/O log file - normally null")
    public String getLogFile();

    /**
     * @param operation
     *            An I/O operation name. Operation names are specified in
     *            {@link #OPERATIONS}.
     * @return Sum of size of all I/O operations of the specified operation
     *         type.
     */
    @Description("Total bytes moved by one operation type (see IOMeterMXBeans.OPERATIONS)")
    public long totalBytes(final String operation);

    /**
     * @param operation
     *            An I/O operation name. Operation names are specified in
     *            {@link #OPERATIONS}.
     * @return Count of size of all I/O operations of the specified operation
     *         type.
     */
    @Description("Total number of operations performed for a specified type (see IOMeterMXBeans.OPERATIONS)")
    public long totalOperations(final String operation);
}
