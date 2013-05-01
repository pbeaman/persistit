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

        final Runnable r1 = new Runnable() {
            @Override
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

        final Runnable r2 = new Runnable() {
            @Override
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
            final Thread t1 = new Thread(r1);
            final Thread t2 = new Thread(r2);
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
            final String history = sequencerHistory();
            /*
             * The history varies, and testing for validity is complicated; The
             * ABCDE test confirms correct execution order
             */
            assertTrue(history.contains("+A"));
        }

        disableSequencer();
    }

}
