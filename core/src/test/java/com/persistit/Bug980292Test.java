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

package com.persistit;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class Bug980292Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/980292
     * 
     * Of primary interest: WARN [main] 2012-03-31 19:03:59,910
     * Slf4jAdapter.java (line 107) [main] WARNING Recovery exception
     * com.persistit.exception.CorruptJournalException: Long record chain
     * missing page 1607394 at count 0 at JournalAddress 292,793,432,061{0} at
     * transaction TStatus 292,793,432,061{0}u
     */

    private static final String TREE_NAME = "Bug980292Test";

    private static Exchange getExchange(Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, true);
    }

    @Test
    public void testBug980292() throws Exception {
        final Exchange ex = getExchange(_persistit);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        /*
         * Create a long record.
         */
        ex.getValue().put(new byte[50000]);
        ex.to("a").store();
        /*
         * Now add enough other records to force out a chain of transaction
         * buffers.
         */
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < txn.getTransactionBuffer().capacity() * 2 / RED_FOX.length(); i++) {
            ex.to(i).store();
        }
        _persistit.checkpoint();
        txn.rollback();
        txn.end();
        final Properties properties = _persistit.getProperties();
        _persistit.getJournalManager().force();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(properties);
        assertEquals(0, _persistit.getRecoveryManager().getErrorCount());
    }
}
