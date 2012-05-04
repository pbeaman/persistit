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

import javax.management.MXBean;

/**
 * Management structure holding information about the state of a
 * {@link BufferPool}, including counters for events such as physical I/O
 * operations, evictions, and hit ratio.
 */
@MXBean
public interface BufferPoolMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=BufferPool";

    /**
     * @return The size of <code>Buffer</code>s managed by this pool
     */
    @Description("The size in bytes of each buffer in this pool")
    public int getBufferSize();

    /**
     * @return The count of <code>Buffer</code>s managed by this pool.
     */
    @Description("The count of Buffer managed by this pool")
    public int getBufferCount();

    /**
     * Return the number of lookup operations for pages that resulted in a
     * physical disk read operation. This number, in comparison with the hit
     * counter, indicates how effective the cache is in reducing disk I/O.
     * 
     * @return The miss count
     */
    @Description("Return the number of lookup operations for pages that resulted in a physical disk read operation.")
    public long getMissCount();

    /**
     * Return the number of lookup operations for pages in this pool for which
     * the page image was already found in this <code>BufferPool</code>. This
     * number, in comparison with the get counter, indicates how effective the
     * cache is in reducing disk I/O.
     * 
     * @return The hit count
     */
    @Description("The number of lookup operations satisfied by pages already present in this BufferPool")
    public long getHitCount();

    /**
     * @return Number of pages newly created in this <code>BufferPool</code>.
     */
    @Description("Number of pages newly created in this BufferPool")
    public long getNewCount();

    /**
     * @return The number of valid pages evicted from this
     *         <code>BufferPool</code> to make room for newly read or created
     *         pages.
     */
    @Description("The number of valid pages evicted from this BufferPool to make room for newly read or created pages.")
    public long getEvictCount();

    /**
     * @return The total number of pages written to disk from this
     *         <code>BufferPool</code>.
     */
    @Description("The total number of pages written to disk from this BufferPool.")
    public long getWriteCount();

    /**
     * @return The number of pages written to disk from this
     *         <code>BufferPool</code> due to checkpoints.
     */
    @Description("The number of pages written to disk from this BufferPool due to checkpoints.")
    public long getForcedCheckpointWriteCount();

    /**
     * @return The number of pages written to disk from this
     *         <code>BufferPool</code> due to eviction.
     */
    @Description("The number of pages written to disk from this BufferPool due to eviction.")
    public long getForcedWriteCount();

    /**
     * Get the "hit ratio" - the number of hits divided by the number of overall
     * gets. A value close to 1.0 indicates that most attempts to find data in
     * the <code>BufferPool</code> are successful - i.e., that the cache is
     * effectively reducing the need for disk read operations.
     * 
     * @return The ratio of pages found in this <code>BufferPool</code> to total
     *         number of pages accessed
     */
    @Description("The ratio of pages found in this BufferPool to total number of pages accessed.")
    public double getHitRatio();

    /**
     * @return The count of valid pages in this <code>BufferPool</code>
     */
    @Description("The count of valid pages in this BufferPool.")
    public int getValidPageCount();

    /**
     * Get the count of dirty pages (pages that contain updates not yet written
     * to disk) in this pool.
     * 
     * @return The count of dirty pages in this pool
     */
    @Description("The count of dirty pages in this BufferPool")
    public int getDirtyPageCount();

    /**
     * Get the count of pages on which running threads have reader
     * (non-exclusive), but <i>not</i> writer (exclusive) claims in this
     * <code>BufferPool</code>.
     * 
     * @return The count of pages with reader claims
     */
    @Description("The count of pages with reader claims.")
    public int getReaderClaimedPageCount();

    /**
     * Get the count of pages on which running threads have writer (exclusive)
     * claims in this <code>BufferPool</code>.
     * 
     * @return The count of pages with writer claims
     */
    @Description("The count of pages with writer claims")
    public int getWriterClaimedPageCount();

    /**
     * @return Earliest timestamp of any dirty page in this
     *         <code>BufferPool</code>.
     */
    @Description("Earliest timestamp of any dirty page in this BufferPool.")
    public long getEarliestDirtyTimestamp();

}
