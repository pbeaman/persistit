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
import com.persistit.stress.unit.Stress4a;
import com.persistit.stress.unit.Stress4b;

public class Stress4Suite extends AbstractSuite {

    static String name() {
        return Stress4Suite.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new Stress4Suite(args).runTest();
    }

    public Stress4Suite(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        if (isUntilStopped()) {
            setDuration(getDuration() / 2);
        }

        for (int pageSize = 8192; pageSize <= 16384; pageSize *= 2) {

            System.out.printf("Starting %s for page size %,d\n", Stress4Suite.class.getSimpleName(), pageSize);

            deleteFiles(substitute("$datapath$/persistit*"));

            add(new Stress4a("count=1000000"));
            add(new Stress4b("count=1000000"));

            final Persistit persistit = makePersistit(pageSize, "10000", CommitPolicy.SOFT);

            try {
                execute(persistit);
            } finally {
                persistit.close();
            }

            clear();
        }
    }
}
