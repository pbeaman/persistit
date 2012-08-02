/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.stress.unit;

import com.persistit.exception.PersistitException;

public class Stress4b extends Stress4Base {

    public Stress4b(String argsString) {
        super(argsString);
    }

    @Override
    public void repeatedTasks() throws PersistitException {
        writeRecords(_total, false, 1000, 1000);
        readRecords(_total, false, 1000, 1000);
        testForward();
        testReverse();
        testReads(_total);
        seed(_seed);
        writeRecords(_total, true, 20, 1000);
        seed(_seed);
        readRecords(_total, true, 20, 1000);
        seed(_seed * 7);
        writeRecords(_total, true, 20, 8000);
        seed(_seed * 7);
        readRecords(_total, true, 20, 8000);
        testReverse();
        seed(_seed * 7);
        writeRecords(_total / 200, true, 20, 80000);
        seed(_seed * 7);
        readRecords(_total / 200, true, 20, 80000);
        testReverse();
        seed(_seed * 17);
        removeRecords(_seed, true);
        testForward();
    }

}
