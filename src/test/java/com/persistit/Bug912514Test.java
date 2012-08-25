/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        for (final String string : sequence) {

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
