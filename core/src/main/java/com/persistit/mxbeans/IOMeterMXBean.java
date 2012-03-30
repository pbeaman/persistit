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

package com.persistit.mxbeans;

import java.io.IOException;

import javax.management.MXBean;

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
     * <dd>Page copy from Journal to Volume</dd>
     * <dt>WJ</dt>
     * <dd>Write page from Journal</dd>
     * <dt>TS</dt>
     * <dd>Transaction Start</dd>
     * <dt>TC</dt>
     * <dd>Transaction Commit</dd>
     * <dt>SR</dt>
     * <dd>Store Record</dd>
     * <dt>DR</dt>
     * <dd>Delete Record or Range</dd>
     * <dt>DT</dt>
     * <dd>Delete Tree</dd>
     * <dt>XX</dt>
     * <dd>Other</dd>
     * <dt>EV</dt>
     * <dd>Evict page from pool</dd>
     * <dt>FJ</dt>
     * <dd>Flush journal</dd>
     * </dl>
     * 
     */
    public final static String[] OPERATIONS = { "??", "RV", "RJ", "CC", "WJ", "TS", "TC", "SR", "DR", "DT", "XX", "EV",
            "FJ", "GP" };

    public final static String[] SUMMARY_ITEMS = { "CC", "RV", "RJ", "WJ", "EV", "FJ" };

    /**
     * @return the writePageSleepInterval
     */
    public long getWritePageSleepInterval();

    /**
     * @param writePageSleepInterval
     *            the writePageSleepInterval to set
     */
    public void setWritePageSleepInterval(long writePageSleepInterval);

    /**
     * Time interval in milliseconds between page copy operations.
     * 
     * @return the CopySleepInterval
     */
    public long getCopyPageSleepInterval();

    /**
     * @param copyPageSleepInterval
     *            the copySleepInterval to set
     */
    public void setCopyPageSleepInterval(long copyPageSleepInterval);

    /**
     * @return the quiescentIOthreshold
     */
    public long getQuiescentIOthreshold();

    /**
     * @param quiescentIOthreshold
     *            the quiescentIOthreshold to set
     */
    public void setQuiescentIOthreshold(long quiescentIO);

    /**
     * @return the ioRate
     */
    public long getIoRate();

    /**
     * Path for file into which IO events should be logged, or <code>null</code>
     * to disable IO logging.
     * 
     * @param toFile
     * @throws IOException
     */
    public void setLogFile(final String toFile) throws IOException;

    /**
     * Path for file into which IO events should be logged, or <code>null</code>
     * if IO logging is disabled.
     * 
     * @return
     */
    public String getLogFile();

    /**
     * @param operation
     *            An I/O operation name. Operation names are specified in
     *            {@link #OPERATIONS}.
     * @return Sum of size of all I/O operations of the specified operation
     *         type.
     */
    public long getSum(final String operation);

    /**
     * @param operation
     *            An I/O operation name. Operation names are specified in
     *            {@link #OPERATIONS}.
     * @return Count of size of all I/O operations of the specified operation
     *         type.
     */
    public long getCount(final String operation);
}
