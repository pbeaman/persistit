/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import com.persistit.AlertMonitor.AlertLevel;

/**
 * Management interface to the <code>AlertMonitor</code> which accumulates and
 * logs abnormal events such as IOExceptions and measurements outside of
 * expected thresholds.
 */
@MXBean
public interface AlertMonitorMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=AlertMonitor";

    final static String MANY_JOURNAL_FILES = "JournalFiles";
    final static String JOURNAL_CATEGORY = "Journal";
    final static String WRITE_PAGE_CATEGORY = "WritePage";
    final static String READ_PAGE_CATEGORY = "ReadPage";
    final static String EXTEND_VOLUME_CATEGORY = "ExtendVolume";
    final static String FLUSH_STATISTICS_CATEGORY = "FlushStatistics";
    final static String CLEANUP_CATEGORY = "Cleanup";
    final static String MISSING_VOLUME_CATEGORY = "MissingVolume";

    /**
     * Current maximum AlertLevel in this monitor as a String: one of NORMAL,
     * WARN or ERROR
     */
    @Description("Current maximum AlertLevel in this monitor as a String: one of NORMAL, WARN or ERROR.")
    String getAlertLevel();

    /**
     * Restore this alert monitor to its initial state at level
     * {@link AlertLevel#NORMAL} with no history.
     */
    @Description("Restore this alert monitor to its initial state.")
    void reset();

    /**
     * @return the interval in milliseconds between successive notifications for
     *         this monitor when its level is {@link AlertLevel#WARN}.
     */
    @Description("The interval in milliseconds between successive notifications"
            + " for this monitor when its level is WARN.")
    long getWarnLogTimeInterval();

    /**
     * Set the interval between successive notifications for this monitor when
     * its {@link AlertLevel#WARN}.
     * 
     * @param warnLogTimeInterval
     *            the interval in milliseconds
     */
    @Description("The interval in milliseconds between successive notifications"
            + " for this monitor when its level is WARN.")
    void setWarnLogTimeInterval(long warnLogTimeInterval);

    /**
     * @return the interval in milliseconds between successive notifications for
     *         this monitor when its {@link AlertLevel#ERROR}.
     */
    @Description("The interval in milliseconds between successive notifications"
            + " for this monitor when its level is ERROR.")
    long getErrorLogTimeInterval();

    /**
     * Set the interval between successive notifications for this monitor when
     * its {@link AlertLevel#ERROR}.
     * 
     * @param errorLogTimeInterval
     *            the interval in milliseconds
     */
    @Description("The interval in milliseconds between successive notifications"
            + " for this monitor when its level is ERROR.")
    void setErrorLogTimeInterval(long errorLogTimeInterval);

    /**
     * @return The number of events per category for which to keep individual
     *         Events.
     */
    @Description("The number of events per category for which to keep individual Events.")
    int getHistoryLength();

    /**
     * Set the number of events per category on which to keep a complete
     * history. Once the number of events exceeds this count, the events
     * aggregated.
     * 
     * @param historyLength
     *            the historyLength to set
     */
    @Description("The number of events per category for which to keep individual Events.")
    void setHistoryLength(@PName("historyLength") int historyLength);

    /**
     * Return a summary of this AlertMonitor's current state.
     * 
     * @return a summary report
     */
    @Description("A summary of this AlertMonitor's current state")
    String getSummary();

    /**
     * Return a detailed description, including first and recent occurrences of
     * events within the History for the specified category. If there is no
     * history for the specified category, this method returns <code>null</code>
     * .
     * 
     * @param category
     *            the category name
     * @return the detailed report
     */
    @Description("A detailed description, including first and recent occurrences of"
            + " events within the History for the specified category")
    String getDetailedHistory(final String category);

    /**
     * Called periodically to emit log messages
     * 
     * @param force
     *            Whether to force notifications to be issued immediatel
     */
    @Description("Operation called periodically to emit notifications and log messages.")
    void poll(
            @PName("force") @Description("Whether to force notifications to be issued immediately") final boolean force);

}
