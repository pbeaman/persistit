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

import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequencerHistory;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

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

        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    _persistit.checkpoint();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        txn.commit();
        txn.end();

        final String history = sequencerHistory();
        disableSequencer();

        // prevents spurious "MissingThreadException" from background thread
        thread.join();

        assertTrue(history.startsWith("+COMMIT_FLUSH_A,+COMMIT_FLUSH_B,-COMMIT_FLUSH_A,+COMMIT_FLUSH_C"));
    }
}
