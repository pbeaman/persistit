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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1056489
 * 
 * When utilizing the step capability of transactions there appear to be
 * inconsistencies between reading, writing, removing, and pruning. Particularly
 * when steps of a single transaction appear out of order in a single MVV.
 * 
 * An invariant in MVV#storeVersion is that each version added to an MVV must
 * have a larger ts than any version already present in the MVV. This follows
 * from ww-dependency validation; if there's a version tsv on the MVV and my ts
 * is before that tsv, by definition I'm concurrent with it and can't write.
 * 
 * But, the step handling code breaks that. A transaction can't conflict with
 * itself, so no code prevents insertion of versions with step numbers out of
 * sequence. MVV#storeVersion does not rearrange the MVV in step order, and the
 * pruning code simply keeps the last value found for each concurrent
 * transaction. Therefore in the test case for key={2}, the AntiValue for the
 * remove is stored after the value 200 in the MVV. The MVV looks like this:
 * 
 * [277<277>:20,282#02<UNCOMMITTED>:200,282#01<UNCOMMITTED>:AntiValue{}]
 * 
 * Pruning will then remove the value at 282#2 and keep 282#1, the AntiValue
 * from the remove.
 * 
 */

public class Bug1056489Test extends PersistitUnitTestCase {

    public void update(final Exchange ex, final int k, final int v) throws Exception {
        _persistit.getTransaction().setStep(2);
        ex.getKey().clear().append(k);
        ex.getValue().clear().put(v);
        ex.store();
    }

    public void remove(final Exchange ex, final int k) throws Exception {
        _persistit.getTransaction().setStep(1);
        ex.getKey().clear().append(k);
        ex.remove();
    }

    @Test
    public void mvvStepCheck() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final Exchange ex = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "new_tree1", true);

        txn.begin();
        for (int i = 1; i <= 3; ++i) {
            ex.getKey().clear().append(i);
            ex.getValue().clear().put(i * 10);
            ex.store();
        }
        txn.commit();
        txn.end();
        treeCheck(ex, "Initial", 10, 20, 30);

        txn.begin();
        for (int i = 1; i <= 3; ++i) {
            ex.clear().append(i);
            if (i == 2) {
                update(ex, i, i * 100);
                remove(ex, i);
            } else {
                remove(ex, i);
                update(ex, i, i * 100);
            }
        }
        treeCheck(ex, "Post update, pre commit", 100, 200, 300);
        txn.commit();
        txn.end();

        treeCheck(ex, "Updated, committed", 100, 200, 300);

        while (_persistit.getCleanupManager().getPerformedCount() < _persistit.getCleanupManager().getAcceptedCount()) {
            Thread.sleep(100);
        }

        treeCheck(ex, "Updated, committed, pruned", 100, 200, 300);

        _persistit.getTransactionIndex().cleanup();
        txn.begin();

        treeCheck(ex, "Updated, committed, TI cleaned", 100, 200, 300);
        ex.clear().append(2);
        assertTrue("Removed should find key {2}", ex.remove());
        txn.commit();
        txn.end();
        treeCheck(ex, "Updated, committed, TI cleaned, removed", 100, 300);
    }

    private void treeCheck(final Exchange ex, final String message, final int... expected) throws PersistitException {
        ex.clear();
        int index = 0;
        while (ex.next(true)) {
            assertTrue("Too many keys", index < expected.length);
            assertEquals("Wrong value", expected[index++], ex.getValue().getInt());

        }
    }
}
