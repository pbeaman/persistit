/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import org.junit.Test;

public class Bug932097Test extends PersistitUnitTestCase {

    @Test
    public void testInjectedAbortTransactionStatus() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "test", true);
        /*
         * Create a bunch of incomplete transactions
         */
        for (int i = 1; i <= 1000; i++) {
            _persistit.setSessionId(new SessionId());
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
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
