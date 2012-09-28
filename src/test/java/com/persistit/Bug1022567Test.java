/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.unit.UnitTestProperties.VOLUME_NAME;
import static com.persistit.util.SequencerConstants.DEALLOCATE_CHAIN_A;
import static com.persistit.util.SequencerConstants.DEALLOCATE_CHAIN_B;
import static com.persistit.util.SequencerConstants.DEALLOCATE_CHAIN_C;
import static com.persistit.util.SequencerConstants.DEALLOCATE_CHAIN_SCHEDULED;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static com.persistit.util.ThreadSequencer.setCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;
import com.persistit.util.ThreadSequencer;
import com.persistit.util.ThreadSequencer.Condition;

/**
 * MixtureTxn1 suite failed with this Exception:
 * 
 * Stress6 [Thread-195] FAILED [Thread-195]: java.lang.IllegalStateException:
 * De-allocating page that is already garbage: root=36689 left=72838 right=36689
 * 
 * at com.persistit.VolumeStructure.deallocateGarbageChain(VolumeStructure.java:
 * 510)
 * 
 * As it turns out, the condition being asserted is a rare but legitimate state and
 * the bug fix it to remove a conditional clause from the assert statement in
 * VolumeStructure#deallocateGarbageChain.  The recreate1022567 method below
 * reproduces the legitimate case, and without the change in VolumeStructure cuases
 * a the IllegalStateException.
 */

public class Bug1022567Test extends PersistitUnitTestCase {

    private final static String TREE_NAME = "Bug1022567";

    private Exchange getExchange() throws PersistitException {
        return _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    /**
     * This method carefully constructs a scenario in which the right sibling of
     * a chain being deallocated is the garbage root page. The reported bug is
     * that this condition causes an assert, when in fact the condition is rare
     * but legitimate. Prior to fixing the bug this method throws an
     * IllegalStateException.
     * 
     * @throws Exception
     */
    @Test
    public void recreate1022567() throws Exception {
        try {
            enableSequencer(true);
            addSchedules(DEALLOCATE_CHAIN_SCHEDULED);

            final long mainThreadId = Thread.currentThread().getId();
            setCondition(DEALLOCATE_CHAIN_A, new Condition() {
                public boolean enabled() {
                    return Thread.currentThread().getId() == mainThreadId;
                }
            });
            for (int loop = 0; loop < 1000; loop++) {
                _persistit.getVolume(VOLUME_NAME).truncate();
                final Exchange ex1 = getExchange();
                final List<Integer> keys = new ArrayList<Integer>();
                /*
                 * Lay down 4 pages
                 */
                long nextAvailable = 0;
                ex1.getValue().put(createString(2000));
                for (int i = 0;; i++) {
                    ex1.clear().append(i).store();
                    final long newNextAvailable = nextAvailablePage();
                    if (newNextAvailable != nextAvailable) {
                        keys.add(i - 1);
                        nextAvailable = newNextAvailable;
                        if (keys.size() > 3) {
                            break;
                        }
                    }
                }
                final Key key1 = new Key(_persistit);
                final Key key2 = new Key(_persistit);
                key1.to(keys.get(1));
                key2.to(keys.get(3));

                // Need to create a race: this thread needs to remove the inside
                // two
                // pages and then let the other thread proceed before
                // deallocating
                // them.

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try {
                            sequence(DEALLOCATE_CHAIN_B);
                            final Exchange ex2 = getExchange();
                            ex2.to(Key.AFTER);
                            while (ex2.previous()) {
                                ex2.remove();
                            }
                            sequence(DEALLOCATE_CHAIN_C);
                        } catch (PersistitException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t.start();
                key1.nudgeLeft();
                key2.nudgeLeft();
                ex1.removeKeyRange(key1, key2);
                t.join();
            }
        } finally {
            ThreadSequencer.disableSequencer();
        }
    }

    @Test
    public void deleteEnlargingRanges() throws Exception {
        final Exchange ex = getExchange();
        final VolumeStructure vs = ex.getVolume().getStructure();
        final List<KeyState> keys = new ArrayList<KeyState>();
        /*
         * Lay down about 1000 pages
         */
        long nextAvailable = 0;
        ex.getValue().put(RED_FOX);
        for (int i = 0;; i++) {
            ex.clear().append(i).store();
            final long newNextAvailable = nextAvailablePage();
            if (newNextAvailable != nextAvailable) {
                keys.add(new KeyState(ex.getKey()));
                nextAvailable = newNextAvailable;
                if (nextAvailable > 1000) {
                    break;
                }
            }
        }
        final List<Thread> threads = new ArrayList<Thread>();
        for (int i = 2; i < 480; i++) {
            final int offset = i;
            final Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        final Key key1 = new Key(_persistit);
                        final Key key2 = new Key(_persistit);
                        keys.get(500 - offset).copyTo(key1);
                        keys.get(500 + offset).copyTo(key2);
                        final Exchange exchange = _persistit.getExchange(VOLUME_NAME, TREE_NAME, false);
                        exchange.removeKeyRange(key1, key2);
                    } catch (PersistitException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
            threads.add(t);
        }
        for (final Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void deleteEnlargingRangesSingleThread() throws Exception {
        final Exchange ex = getExchange();
        final VolumeStructure vs = ex.getVolume().getStructure();
        final List<KeyState> keys = new ArrayList<KeyState>();
        /*
         * Lay down about 1000 pages
         */
        long nextAvailable = 0;
        ex.getValue().put(createString(2000));
        for (int i = 0;; i++) {
            ex.clear().append(i).store();
            final long newNextAvailable = nextAvailablePage();
            if (newNextAvailable != nextAvailable) {
                keys.add(new KeyState(ex.getKey()));
                nextAvailable = newNextAvailable;
                if (nextAvailable > 1000) {
                    break;
                }
            }
        }
        for (int i = 2; i < 80; i++) {
            final int offset = i;
            try {
                final Key key1 = new Key(_persistit);
                final Key key2 = new Key(_persistit);
                keys.get(500 - offset).copyTo(key1);
                keys.get(500 + offset).copyTo(key2);
                final Exchange exchange = _persistit.getExchange(VOLUME_NAME, TREE_NAME, false);
                exchange.removeKeyRange(key1, key2);
            } catch (PersistitException e) {
                e.printStackTrace();
            }
        }
    }

    private long nextAvailablePage() throws PersistitException {
        return _persistit.getVolume(VOLUME_NAME).getStorage().getNextAvailablePage();
    }

}
