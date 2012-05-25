/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequencerHistory;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.ThreadSequencer;

public class Bug937877Test extends PersistitUnitTestCase {

    /**
     * Test for a race condition that probably caused 937877. This test depends
     * on strategic placement of {@link ThreadSequencer#sequence(int)}
     * statements in the Transaction class. If those statements are moved or
     * modified, this test will probably need to be changed.
     * 
     * @throws Exception
     */
    @Test
    public void testCommitRaceCondition() throws Exception {

        enableSequencer(true);
        addSchedules(COMMIT_FLUSH_SCHEDULE);

        final Exchange ex = _persistit.getExchange("persistit", "test", true);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int k = 1; k < 10; k++) {
            ex.clear().append(k).store();
        }

        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    _persistit.checkpoint();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        txn.commit();
        txn.end();

        String history = sequencerHistory();
        disableSequencer();

        // prevents spurious "MissingThreadException" from background thread
        thread.join();

        assertTrue(history.startsWith("+COMMIT_FLUSH_A,+COMMIT_FLUSH_B,-COMMIT_FLUSH_A,+COMMIT_FLUSH_C"));
    }
}
