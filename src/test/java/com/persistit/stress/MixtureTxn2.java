/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.stress;

import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress1;
import com.persistit.stress.unit.Stress2txn;
import com.persistit.stress.unit.Stress3;
import com.persistit.stress.unit.Stress3txn;
import com.persistit.stress.unit.Stress5;
import com.persistit.stress.unit.Stress6;
import com.persistit.stress.unit.Stress8txn;

public class MixtureTxn2 extends AbstractSuite {

    static String name() {
        return MixtureTxn2.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new MixtureTxn2(args).runTest();
    }

    public MixtureTxn2(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress1("repeat=10 count=25000"));
        add(new Stress1("repeat=10 count=25000"));
        add(new Stress3("repeat=5 count=25000 seed=119"));
        add(new Stress5("repeat=5 count=25000"));
        add(new Stress6("repeat=5 count=1000 size=250"));
        add(new Stress6("repeat=10 count=1000 size=250"));

        for (int i = 0; i < 10; i++) {
            add(new Stress2txn("repeat=10 count=2500 size=4000 seed=1" + i));
            add(new Stress3txn("repeat=5 count=25000 seed=2" + i));
            add(new Stress8txn("repeat=10 count=1000 size=1000 seed=3" + i));
        }

        final Persistit persistit = makePersistit(16384, "10000", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
