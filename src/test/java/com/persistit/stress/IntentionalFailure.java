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

public class IntentionalFailure extends AbstractSuite {

    static String name() {
        return IntentionalFailure.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new IntentionalFailure(args).runTest();
    }

    public IntentionalFailure(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Fail(null));

        final Persistit persistit = makePersistit(16384, "1000", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
    
    class Fail extends AbstractStressTest {

        protected Fail(String argsString) {
            super(argsString);
        }

        @Override
        protected void executeTest() throws Exception {
            throw new RuntimeException("Intentional Failure");
        }
        
    }
}
