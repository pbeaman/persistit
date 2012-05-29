/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.mxbeans;

import javax.management.MXBean;

/**
 * Management structure holding information about the state of a
 * <code>BufferPool</code>, including counters for events such as physical I/O
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
