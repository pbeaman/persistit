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

package com.persistit.stress;

import com.persistit.IntegrityCheck;
import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress3;
import com.persistit.stress.unit.Stress3txn;

public class StartStop extends AbstractSuite {

    static String name() {
        return StartStop.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new StartStop(args).runTest();
    }

    public StartStop(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {
        final long end = System.nanoTime() + getDuration() * NS_PER_S;
        int cycle = 0;
        final boolean untilStopped = takeUntilStopped();

        while (cycle++ == 0 || untilStopped && System.nanoTime() < end) {

            deleteFiles(substitute("$datapath$/persistit*"));
            Persistit persistit = null;

            for (int stage = 0; stage < 6; stage++) {
                persistit = makePersistit(16384, "12000", CommitPolicy.SOFT);
                try {
                    switch (stage) {
                    case 0:
                        break;
                    case 1:
                        break;
                    case 2:
                        add(new Stress3("repeat=1 count=250 seed=331"));
                        execute(persistit);
                        clear();
                        confirmIntegrity(persistit);
                        break;
                    case 3:
                        add(new Stress3txn("repeat=1 count=250 seed=331"));
                        execute(persistit);
                        clear();
                        confirmIntegrity(persistit);
                        break;
                    case 4:
                        confirmIntegrity(persistit);
                        persistit.close();
                        break;
                    case 5:
                        add(new Stress3txn("repeat=1 count=250 seed=331"));
                        execute(persistit);
                        clear();
                        confirmIntegrity(persistit);
                        break;
                    default:
                        throw new RuntimeException("Missing case: " + stage);
                    }

                } finally {
                    persistit.close();
                }
            }
        }

    }

    static void confirmIntegrity(final Persistit persistit) throws Exception {
        final IntegrityCheck icheck = IntegrityCheck.icheck("persistit:*", false, false, false, false, false, false,
                false);
        icheck.setPersistit(persistit);
        icheck.setMessageWriter(null);
        icheck.run();
        if (icheck.getFaults().length > 0) {
            throw new RuntimeException(icheck.getFaults().length + " faults");
        }
    }
}
