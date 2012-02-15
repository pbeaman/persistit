/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.bug;

import java.util.Properties;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;


public class Bug932097Test extends PersistitUnitTestCase {

    public void testInjectedAbortTransactionStatus() throws Exception {
        /*
         * Create a bunch of incomplete transactions
         */
        for (int i = 1; i <= 1000; i++) {
            _persistit.setSessionId(TestShim.newSessionId());
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
         * To exploit the bug, register a bunch of new transactions which will draw
         * the TransactionStatus instances freed after recovery finishes its rollback
         * processing. With bug, we expect to see a 60-second pause followed by
         * an assertion.
         */
        for (int i = 1; i <= 1000; i++) {
            _persistit.setSessionId(TestShim.newSessionId());
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
            txn.commit();
            txn.end();
        }
    }
}
