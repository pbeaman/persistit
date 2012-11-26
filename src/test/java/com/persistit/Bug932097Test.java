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

package com.persistit;

import org.junit.Test;

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
        _persistit.crash();
        _persistit = new Persistit(_config);
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
