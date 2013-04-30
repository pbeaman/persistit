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
import com.persistit.stress.unit.Stress12txn;

public class Stress12txnSuite extends AbstractSuite {

    static String name() {
        return Stress12txnSuite.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new Stress12txnSuite(args).runTest();
    }

    public Stress12txnSuite(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress12txn("repeat=50 count=250000 size=100 seed=1"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=2"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=3"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=4"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=5"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=6"));

        final Persistit persistit = makePersistit(16384, "20K", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
