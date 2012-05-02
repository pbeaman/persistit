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

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug932097Test extends PersistitUnitTestCase {

    @Test
    public void testInjectedAbortTransactionStatus() throws Exception {
        /*
         * Create a bunch of incomplete transactions
         */
        for (int i = 1; i <= 1000; i++) {
            _persistit.setSessionId(new SessionId());
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
            final Exchange ex = _persistit.getExchange("persistit", "test", true);
            ex.getValue().put(RED_FOX);
            txn.begin();
            for (int k = 1; k < 10; k++) {
                ex.clear().append(i).append(k).store();
            }
            /*
             * Neither commit nor rollback
             */
        }
        _persistit.checkpoint();
        final Properties properties = _persistit.getProperties();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(properties);
        /*
         * To exploit the bug, register a bunch of new transactions which will
         * draw the TransactionStatus instances freed after recovery finishes
         * its rollback processing. With bug, we expect to see a 60-second pause
         * followed by an assertion.
         */
        for (int i = 1; i <= 1000; i++) {
            _persistit.setSessionId(new SessionId());
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
            txn.commit();
            txn.end();
        }
    }
}
