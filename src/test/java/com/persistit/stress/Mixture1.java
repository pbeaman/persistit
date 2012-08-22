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
import com.persistit.stress.unit.Stress1;
import com.persistit.stress.unit.Stress2;
import com.persistit.stress.unit.Stress3;
import com.persistit.stress.unit.Stress5;
import com.persistit.stress.unit.Stress6;
import com.persistit.stress.unit.Stress7;

public class Mixture1 extends AbstractSuite {

    static String name() {
        return Mixture1.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new Mixture1(args).runTest();
    }

    public Mixture1(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress1("repeat=1 count=1000000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=1 count=100000 seed=3"));
        add(new Stress2("repeat=1 count=100000 seed=4"));
        add(new Stress3("repeat=5 count=100000 seed=5"));
        add(new Stress3("repeat=5 count=100000 seed=6"));
        add(new Stress5("repeat=5 count=1000000"));
        add(new Stress5("repeat=5 count=1000000"));
        add(new Stress6("repeat=1 count=25000 size=250"));
        add(new Stress6("repeat=3 count=50000 size=250"));
        add(new Stress7("repeat=1 count=60000 size=250 seed=11"));
        add(new Stress7("repeat=10 count=6000 size=250 seed=12"));

        final Persistit persistit = makePersistit(16384, "1000", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
