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

package com.persistit;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
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
