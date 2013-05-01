/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

public class Mixture3 extends AbstractSuite {

    static String name() {
        return Mixture3.class.getSimpleName();
    }

    public static void main(final String[] args) throws Exception {
        new Mixture3(args).runTest();
    }

    public Mixture3(final String[] args) {
        super(name(), args);
    }

    @Override
    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=10000"));
        add(new Stress2("repeat=5 count=100000 seed=117"));
        add(new Stress2("repeat=5 count=100000 seed=118"));
        add(new Stress3("repeat=5 count=100000 seed=119"));
        add(new Stress3("repeat=5 count=100000 seed=120"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=217"));
        add(new Stress2("repeat=5 count=100000 seed=218"));
        add(new Stress3("repeat=5 count=100000 seed=219"));
        add(new Stress3("repeat=5 count=100000 seed=220"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=121"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=122"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=317"));
        add(new Stress2("repeat=5 count=100000 seed=318"));
        add(new Stress3("repeat=5 count=100000 seed=319"));
        add(new Stress3("repeat=5 count=100000 seed=320"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=417"));
        add(new Stress2("repeat=5 count=100000 seed=418"));
        add(new Stress3("repeat=5 count=100000 seed=419"));
        add(new Stress3("repeat=5 count=100000 seed=420"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=221"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=222"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=517"));
        add(new Stress2("repeat=5 count=100000 seed=518"));
        add(new Stress3("repeat=5 count=100000 seed=519"));
        add(new Stress3("repeat=5 count=100000 seed=520"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=617"));
        add(new Stress2("repeat=5 count=100000 seed=618"));
        add(new Stress3("repeat=5 count=100000 seed=619"));
        add(new Stress3("repeat=5 count=100000 seed=620"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=321"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=322"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=717"));
        add(new Stress2("repeat=5 count=100000 seed=718"));
        add(new Stress3("repeat=5 count=100000 seed=719"));
        add(new Stress3("repeat=5 count=100000 seed=720"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=817"));
        add(new Stress2("repeat=5 count=100000 seed=818"));
        add(new Stress3("repeat=5 count=100000 seed=819"));
        add(new Stress3("repeat=5 count=100000 seed=820"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=4121"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=422"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=917"));
        add(new Stress2("repeat=5 count=100000 seed=918"));
        add(new Stress3("repeat=5 count=100000 seed=919"));
        add(new Stress3("repeat=5 count=100000 seed=920"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=1017"));
        add(new Stress2("repeat=5 count=100000 seed=1018"));
        add(new Stress3("repeat=5 count=100000 seed=1019"));
        add(new Stress3("repeat=5 count=100000 seed=1020"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=521"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=522"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=1117"));
        add(new Stress2("repeat=5 count=100000 seed=1118"));
        add(new Stress3("repeat=5 count=100000 seed=1119"));
        add(new Stress3("repeat=5 count=100000 seed=1120"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=1217"));
        add(new Stress2("repeat=5 count=100000 seed=1218"));
        add(new Stress3("repeat=5 count=100000 seed=1219"));
        add(new Stress3("repeat=5 count=100000 seed=1220"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=621"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=622"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=1317"));
        add(new Stress2("repeat=5 count=100000 seed=1318"));
        add(new Stress3("repeat=5 count=100000 seed=1319"));
        add(new Stress3("repeat=5 count=100000 seed=1320"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress1("repeat=10 count=100000"));
        add(new Stress2("repeat=5 count=100000 seed=1417"));
        add(new Stress2("repeat=5 count=100000 seed=1418"));
        add(new Stress3("repeat=5 count=100000 seed=1419"));
        add(new Stress3("repeat=5 count=100000 seed=1420"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress5("repeat=20 count=20000"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress6("repeat=10 count=5000 size=250"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=721"));
        add(new Stress7("repeat=10 count=5000 size=250 seed=722"));

        final Persistit persistit = makePersistit(16384, "50000", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
