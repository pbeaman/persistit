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

/**
 * Simple interface for attaching an external logger.
 * 
 * @author peter
 * 
 */
public interface PersistitLogger {

    /**
     * Write the message to the log at the specified level. Configuration will
     * ensure that this method is called only when the level is indeed loggable.
     * 
     * @param level
     *            The level of the log event
     * @param message
     *            A string-valued description of the event
     */
    void log(final PersistitLevel level, final String message);

    /**
     * Test whether a message at the specified level should be written to the
     * log. This method is called during configuration, not once per log
     * message.
     * 
     * @param level
     *            The level of the log event
     * @return whether a message should be logged at the specified level
     */
    boolean isLoggable(final PersistitLevel level);

    /**
     * Called when Persistit starts using the log.
     * 
     * @throws Exception
     */
    void open() throws Exception;

    /**
     * Called when Persistit stops using the log.
     * 
     * @throws Exception
     */
    void close() throws Exception;

    /**
     * Flush pending ouput
     */
    void flush();
}
