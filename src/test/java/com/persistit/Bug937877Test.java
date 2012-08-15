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

import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequencerHistory;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

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
