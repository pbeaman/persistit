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
import com.persistit.stress.unit.PersistitMapStress1;

public class PersistitMap1 extends AbstractSuite {

    static String name() {
        return PersistitMap1.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new PersistitMap1(args).runTest();
    }

    public PersistitMap1(final String[] args) {
        super(name(), args);
    }

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
