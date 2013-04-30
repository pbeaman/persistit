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

import com.persistit.Configuration;
import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress1;
import com.persistit.stress.unit.Stress2txn;
import com.persistit.stress.unit.Stress3;
import com.persistit.stress.unit.Stress3txn;
import com.persistit.stress.unit.Stress5;
import com.persistit.stress.unit.Stress6;
import com.persistit.stress.unit.Stress8txn;

public class PreloadMixtureTxn1 extends AbstractSuite {
    private final static int CYCLES = 4;

    static String name() {
        return PreloadMixtureTxn1.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new PreloadMixtureTxn1(args).runTest();
    }

    public PreloadMixtureTxn1(final String[] args) {
        super(name(), args);
        setDuration(getDuration() / CYCLES);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        for (int iteration = 0; iteration < CYCLES; iteration++) {
            clear();
            add(new Stress1("repeat=10 count=25000"));
            add(new Stress1("repeat=10 count=25000"));
            add(new Stress2txn("repeat=10 count=2500 size=4000 seed=118"));
            add(new Stress2txn("repeat=2 count=25000 seed=119"));
            add(new Stress3("repeat=5 count=25000 seed=119"));
            add(new Stress3txn("repeat=5 count=25000 seed=120"));
            add(new Stress3txn("repeat=5 count=25000"));
            add(new Stress5("repeat=5 count=25000"));
            add(new Stress6("repeat=5 count=1000 size=250"));
            add(new Stress6("repeat=10 count=1000 size=250"));
            add(new Stress8txn("repeat=2 count=1000 size=1000 seed=1"));
            add(new Stress8txn("repeat=2 count=1000 size=1000 seed=2"));
            add(new Stress8txn("repeat=2 count=1000 size=1000 seed=3"));
            add(new Stress8txn("repeat=2 count=1000 size=1000 seed=4"));

            final Configuration configuration = makeConfiguration(16384, "25000", CommitPolicy.SOFT);
            configuration.setLogFile(configuration.getLogFile() + "_" + iteration);
            configuration.setBufferInventoryEnabled(true);
            configuration.setBufferPreloadEnabled(true);

            final Persistit persistit = new Persistit(configuration);

            try {
                execute(persistit);
            } finally {
                persistit.close();
            }
        }
    }
}
