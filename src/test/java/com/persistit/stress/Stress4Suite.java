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
import com.persistit.stress.unit.Stress4a;
import com.persistit.stress.unit.Stress4b;

public class Stress4Suite extends AbstractSuite {

    static String name() {
        return Stress4Suite.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new Stress4Suite(args).runTest();
    }

    public Stress4Suite(final String[] args) {
        super(name(), args);
    }

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
