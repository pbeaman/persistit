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

import static org.junit.Assert.*;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/912514
 * 
 * PersistitMapStress1 fails intermittently with messages like this:
 * 
 * Finished unit=#1 PersistitMapStress test=PersistitMapStress1 main at ts=8036
 * - elapsed=5738 - FAILED: value not expected to be null 8036 Failed test
 * unit=#1 PersistitMapStress test=PersistitMapStress1 main value not expected
 * to be null
 * 
 * I didn't track down the responsible code, but I'm pretty sure the contents of
 * _spareValue have been mangled.
 * 
 * Since akiban-server does not use this method the bug is no more than medium
 * priority. But it does need to be fixed before akiban-persistit is released as
 * a standalone library.
 */

public class Bug912514Test extends PersistitUnitTestCase {
    
    private final static String ROLLBACK = "rollback";
    
    private void fetchAndStoreAndRemoveHelper(final boolean inTxn, final String... sequence) throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final Exchange exchange = _persistit.getExchange("persistit", "Bug912514Test", true);
        String previous = null;
        for (String string : sequence) {

            if (inTxn) {
                txn.begin();
            }
            
            if (string == null) {
                exchange.to(1).fetchAndRemove();
            } else {
                exchange.getValue().put(string);
                exchange.to(1).fetchAndStore();
            }
            compare(previous, exchange.getValue());

            if (inTxn) {
                if (string.startsWith(ROLLBACK)) {
                    txn.rollback();
                } else {
                    txn.commit();
                }
                txn.end();
                compare(previous, exchange.getValue());
            }

            if (!inTxn || !string.startsWith(ROLLBACK)) {
                previous = string;
            }
            exchange.fetch();
            compare(previous, exchange.getValue());


        }
    }

    private void compare(final String string, final Value value) {
        if (string == null) {
            assertTrue("Value should be undefined", !value.isDefined());
        } else {
            assertEquals("Value should match", string, value.getString());
        }
    }

    @Test
    public void fetchAndStoreTxn() throws Exception {
        fetchAndStoreAndRemoveHelper(true, RED_FOX, createString(100), createString(1000), createString(10000), RED_FOX);
    }

    @Test
    public void fetchAndRemoveNonTxn() throws Exception {
        fetchAndStoreAndRemoveHelper(false, RED_FOX, null, null, createString(100), null, createString(1000), null,
                createString(10000), null, null, RED_FOX, null);
    }

    @Test
    public void fetchAndStoreTxnWithRollbacks() throws Exception {
        fetchAndStoreAndRemoveHelper(true, RED_FOX, createString(100), ROLLBACK, createString(1000), ROLLBACK
                + createString(10000), createString(10000), RED_FOX);
    }

}
