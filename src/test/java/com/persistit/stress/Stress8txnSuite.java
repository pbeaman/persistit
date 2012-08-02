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
import com.persistit.stress.unit.Stress8txn;

public class Stress8txnSuite extends AbstractSuite {

    static String name() {
        return Stress8txnSuite.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new Stress8txnSuite(args).runTest();
    }

    public Stress8txnSuite(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=1"));
        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=2"));
        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=3"));
        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=4"));
        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=5"));
        add(new Stress8txn("repeat=10 count=25000 size=10000 seed=6"));

        final Persistit persistit = makePersistit(16384, "20K", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
