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
import com.persistit.stress.unit.StressRecovery;

public class Recovery1 extends AbstractSuite {

    static String name() {
        return Recovery1.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new Recovery1(args).runTest();
    }

    public Recovery1(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        for (int thread = 0; thread < 5; thread++) {
            add(new StressRecovery(null));
        }

        final Persistit persistit = makePersistit(16384, "64M,1T,256M,0.7", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
