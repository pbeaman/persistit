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

import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress1;
import com.persistit.stress.unit.Stress2txn;
import com.persistit.stress.unit.Stress3;
import com.persistit.stress.unit.Stress3txn;
import com.persistit.stress.unit.Stress5;
import com.persistit.stress.unit.Stress6;
import com.persistit.stress.unit.Stress8txn;

public class MixtureTxn2 extends AbstractSuite {

    static String name() {
        return MixtureTxn2.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new MixtureTxn2(args).runTest();
    }

    public MixtureTxn2(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress1("repeat=10 count=25000"));
        add(new Stress1("repeat=10 count=25000"));
        add(new Stress3("repeat=5 count=25000 seed=119"));
        add(new Stress5("repeat=5 count=25000"));
        add(new Stress6("repeat=5 count=1000 size=250"));
        add(new Stress6("repeat=10 count=1000 size=250"));

        for (int i = 0; i < 10; i++) {
            add(new Stress2txn("repeat=10 count=2500 size=4000 seed=1" + i));
            add(new Stress3txn("repeat=5 count=25000 seed=2" + i));
            add(new Stress8txn("repeat=10 count=1000 size=1000 seed=3" + i));
        }

        final Persistit persistit = makePersistit(16384, "10000", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
