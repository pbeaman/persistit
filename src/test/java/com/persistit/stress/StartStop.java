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

package com.persistit.stress;

import static com.persistit.util.Util.NS_PER_S;

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
