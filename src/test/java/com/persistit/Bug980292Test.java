/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.exception.PersistitException;
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

    private static Exchange getExchange(final Persistit persistit) throws PersistitException {
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
        _persistit.getJournalManager().force();
        _persistit.crash();
        _persistit = new Persistit(_config);
        assertEquals(0, _persistit.getRecoveryManager().getErrorCount());
    }
}
