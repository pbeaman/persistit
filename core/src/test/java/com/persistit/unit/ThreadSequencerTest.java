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

package com.persistit.unit;

import static com.persistit.util.ThreadSequencer.addSchedule;
import static com.persistit.util.ThreadSequencer.allocate;
import static com.persistit.util.ThreadSequencer.array;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static com.persistit.util.ThreadSequencer.sequencerHistory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.util.Debug;

public class ThreadSequencerTest {

    final static int A = allocate("A");
    final static int B = allocate("B");
    final static int C = allocate("C");
    final static int D = allocate("D");
    final static int E = allocate("E");
    final static int F = allocate("F");
    final static int X = allocate("X");

    @Test
    public void sequenceTwoThreads() throws Exception {
        enableSequencer(true);
        addSchedule(array(A, B, C), array(A));
        addSchedule(array(F, B, C), array(B));
        addSchedule(array(F, C, D), array(C));
        addSchedule(array(F, D, E), array(D));
        addSchedule(array(E, X), array(X, E, F));

        final StringBuilder sb = new StringBuilder();

        Runnable r1 = new Runnable() {
            public void run() {
                Debug.debugPause(0.2f, 10);
                sequence(C);
                sb.append("C");
                Debug.debugPause(0.2f, 10);
                sequence(E);
                sb.append("E");
                Debug.debugPause(0.2f, 10);
            }
        };

        Runnable r2 = new Runnable() {
            public void run() {
                Debug.debugPause(0.2f, 10);
                sequence(B);
                sb.append("B");
                Debug.debugPause(0.2f, 10);
                sequence(D);
                sb.append("D");
                Debug.debugPause(0.2f, 10);
                sequence(X);
            }
        };

        for (int count = 0; count < 10; count++) {
            sb.setLength(0);
            Thread t1 = new Thread(r1);
            Thread t2 = new Thread(r2);
            t1.setDaemon(true);
            t2.setDaemon(true);

            t1.start();
            t2.start();

            Thread.sleep(100);

            sb.append("A");
            sequence(A);
            sequence(F);

            t1.join(1000);
            t2.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals("ABCDE", sb.toString());
            String history = sequencerHistory();
            /*
             * The history varies, and testing for validity is complicated; The
             * ABCDE test confirms correct execution order
             */
            assertTrue(history.contains("+A"));
        }
        
        disableSequencer();
    }

}
