package com.persistit.suite;

import com.persistit.Configuration;
import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.Stress1;
import com.persistit.stress.Stress2txn;
import com.persistit.stress.Stress3;
import com.persistit.stress.Stress3txn;
import com.persistit.stress.Stress5;
import com.persistit.stress.Stress6;
import com.persistit.stress.Stress8txn;
import com.persistit.test.RunnerBase;

/*
 logpath $logpath$/result_$timestamp$.log


 #Stress-11 test mixture, multithread, large pool, transactions

 delete $datapath$/persistit*

 clearprops
 prop buffer.count.8192=10000
 prop volume.1=$datapath$/persistit,create,pageSize:8192,initialPages:10K,extensionPages:10K,maximumPages:1M
 prop journalpath=$datapath$/persistit_journal
 prop logfile=$logpath$/persistit_$timestamp$.log
 prop rmiport=8081

 init
 multithread
 test com.persistit.stress.Stress1 repeat=10 count=25000
 test com.persistit.stress.Stress1 repeat=10 count=25000
 test com.persistit.stress.Stress1txn repeat=10 count=25000
 test com.persistit.stress.Stress2txn repeat=2 count=2500 size=4000 seed=118
 test com.persistit.stress.Stress2txn repeat=5 count=25000 seed=119
 test com.persistit.stress.Stress3 repeat=5 count=25000 seed=119
 test com.persistit.stress.Stress3txn repeat=5 count=25000 seed=120
 test com.persistit.stress.Stress3txn repeat=5 count=25000
 test com.persistit.stress.Stress5 repeat=5 count=25000
 test com.persistit.stress.Stress6 repeat=10 count=1000 size=250
 test com.persistit.stress.Stress6 repeat=10 count=1000 size=250
 test com.persistit.stress.Stress8txn repeat=2 count=1000 size=1000 seed=1
 test com.persistit.stress.Stress8txn repeat=2 count=1000 size=1000 seed=2
 test com.persistit.stress.Stress8txn repeat=2 count=1000 size=1000 seed=3
 test com.persistit.stress.Stress8txn repeat=2 count=1000 size=1000 seed=4
 icheck persistit
 waitIfFailed Please examine the failure!
 close 
 */
public class MixtureTxn1 extends RunnerBase {

    public static void main(String[] args) throws Exception {
        new MixtureTxn1(args).runTest();
    }

    private MixtureTxn1(final String[] args) {
        super("MixtureTxn1", args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress1("repeat=10 count=25000"));
        add(new Stress1("repeat=10 count=25000"));
        add(new Stress2txn("repeat=2 count=2500 size=4000 seed=118"));
        add(new Stress2txn("repeat=5 count=25000 seed=119"));
        add(new Stress3("repeat=5 count=25000 seed=119"));
        add(new Stress3txn("repeat=5 count=25000 seed=120"));
        add(new Stress3txn("repeat=5 count=25000"));
        add(new Stress5("repeat=5 count=25000"));
        add(new Stress6("repeat=10 count=1000 size=250"));
        add(new Stress6("repeat=10 count=1000 size=250"));
        add(new Stress8txn("repeat=2 count=1000 size=1000 seed=1"));
        add(new Stress8txn("repeat=2 count=1000 size=1000 seed=2"));
        add(new Stress8txn("repeat=2 count=1000 size=1000 seed=3"));
        add(new Stress8txn("repeat=2 count=1000 size=1000 seed=4"));

        final Persistit persistit = new Persistit();
        final Configuration configuration = makeConfiguration(16384, "1000", CommitPolicy.SOFT);
        
        persistit.initialize(configuration);
        
        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
