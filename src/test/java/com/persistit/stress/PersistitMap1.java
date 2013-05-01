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
import com.persistit.stress.unit.PersistitMapStress1;

public class PersistitMap1 extends AbstractSuite {

    static String name() {
        return PersistitMap1.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new PersistitMap1(args).runTest();
    }

    public PersistitMap1(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new PersistitMapStress1("repeat=1 count=1000000"));
        add(new PersistitMapStress1("repeat=1 count=1000000 -t"));
        add(new PersistitMapStress1("repeat=1 count=1000000"));
        add(new PersistitMapStress1("repeat=1 count=1000000 -t"));
        add(new PersistitMapStress1("repeat=1 count=1000000 splay=17"));
        add(new PersistitMapStress1("repeat=1 count=1000000 -t splay=17"));
        add(new PersistitMapStress1("repeat=1 count=1000000 splay=179"));
        add(new PersistitMapStress1("repeat=1 count=1000000 -t splay=179"));

        final Persistit persistit = makePersistit(16384, "8K", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
