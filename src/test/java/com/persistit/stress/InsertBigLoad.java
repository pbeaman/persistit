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
import com.persistit.TreeBuilder;
import com.persistit.stress.unit.BigLoad;
import com.persistit.stress.unit.BigLoad.BigLoadTreeBuilder;
import com.persistit.util.Util;

public class InsertBigLoad extends AbstractSuite {

    static String name() {
        return InsertBigLoad.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new InsertBigLoad(args).runTest();
    }

    public InsertBigLoad(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        final Configuration config = makeConfiguration(16384, "50000", CommitPolicy.SOFT);
        config.setTmpVolMaxSize(10000000000l);
        final Persistit persistit = new Persistit(config);
        final TreeBuilder tb = new BigLoadTreeBuilder(persistit);

        add(new BigLoad(tb, "records=10000000"));
        add(new BigLoad(tb, "records=10000000"));
        add(new BigLoad(tb, "records=10000000"));
        add(new BigLoad(tb, "records=10000000"));
        add(new BigLoad(tb, "records=10000000"));

        try {
            execute(persistit);
            final long start = System.nanoTime();
            tb.merge();
            final long elapsed = System.nanoTime() - start;
            System.out.printf("Merge took %,dms", elapsed / Util.NS_PER_MS);

        } finally {
            persistit.close();
        }
    }
}
