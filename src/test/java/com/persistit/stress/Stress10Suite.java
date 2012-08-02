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

import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress10;

public class Stress10Suite extends AbstractSuite {

    static String name() {
        return Stress10Suite.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new Stress10Suite(args).runTest();
    }

    public Stress10Suite(final String[] args) {
        super(name(), args);
    }

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
