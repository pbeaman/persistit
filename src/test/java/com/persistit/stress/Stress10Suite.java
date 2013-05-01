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
import com.persistit.stress.unit.Stress10;

public class Stress10Suite extends AbstractSuite {

    static String name() {
        return Stress10Suite.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new Stress10Suite(args).runTest();
    }

    public Stress10Suite(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        if (isUntilStopped()) {
            setDuration(getDuration() / 5);
        }
        for (int pageSize = 1024; pageSize <= 16384; pageSize *= 2) {

            System.out.printf("Starting %s for page size %,d\n", name(), pageSize);

            deleteFiles(substitute("$datapath$/persistit*"));

            add(new Stress10("repeat=1 count=50000 size=3000 seed=1"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=2"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=3"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=4"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=5"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=6"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=7"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=8"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=9"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=10"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=11"));
            add(new Stress10("repeat=1 count=50000 size=3000 seed=12"));

            final Persistit persistit = makePersistit(pageSize, Integer.toString(256 * 1024 * 1024 / pageSize),
                    CommitPolicy.SOFT);

            try {
                execute(persistit);
            } finally {
                persistit.close();
            }

            clear();
            System.out.println();
        }
    }
}
