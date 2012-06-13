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
