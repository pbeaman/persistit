/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */


package com.persistit.newtxn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class is a mock-up of Serializable Snapshot Isolation (See Cahill, Rohm
 * and Fekete paper in TODS 12/2009.) This class has no practical value - it is
 * merely an exercise to verify that we understand the underlying algorithms
 * well enough to implement them correctly.
 * 
 * Snapshot isolation is an MVCC concurrency mechanism in which every value read  
 * by a transaction is the version committed most recently prior to the
 * transaction start. To achieve serializability requires two additional
 * constraints:
 * 
 * 1. FCW (first committer wins), meaning that if two concurrent transactions
 * attempt to update the same value, the first one "wins" and the other must be
 * aborted.
 * 
 * 2. Elimination of "dangerous structures" as defined by Fekete. A dangerous
 * structure is a sequence of two rw-dependency arcs between concurrent
 * transactions. Eliminating one of the arcs by aborting a transaction
 * eliminates the sequence. This may abort transactions unnecessarily, but it
 * does ensure serializability.
 * 
 * This class creates "transaction proposals", each of which has a read set and
 * a write set that contains members of a 22-variable set: A-T, Y and Z. The
 * read and write sets are picked at random, with sizes limited by the readSize
 * writeSize parameters. Variables Y and Z are used with special "write skew"
 * transactions designed to emulate the cases described by Fekete and Cahill
 * (and Berenson in the original SI paper) in which non-overlapping write sets
 * cause non-serializable behavior.
 * 
 * Transaction proposals are created and inserted into a list at random. The
 * list is allowed to grow to a maximum size determined by the concurrency
 * parameter. At random, a transaction from the list is removed and "committed",
 * meaning its serializability conditions are tested and it is marked either
 * "committed" or "aborted".
 * 
 * The "database" is an in-memory map containing tuples for all values of all
 * keys (A-T, Y and Z). After the "concurrent" execution" of all transactions
 * this simulator then clears the "database" and runs them again in a serial
 * order. At the end it verifies that the database state resulting from serial
 * execution is the same as that created by the "concurrent" execution of the
 * transactions.
 * 
 * @author peter
 * 
 */
public class TxnModel {

    Random random;

    int timestampCounter;

    int txnIdCounter;

    Proposal[] allProposals;

    Map<String, List<Tuple>> database = new HashMap<String, List<Tuple>>();

    Map<String, Integer> values = new TreeMap<String, Integer>();

    SortedMap<Integer, Proposal> committedProposals = new TreeMap<Integer, Proposal>();

    private final Proposal PRIMORDIAL_PROPOSAL = new Proposal();

    private final static int PERCENT_SPECIAL_SKEW = 10;

    private int readSize = 3;

    private int writeSize = 3;

    private int concurrency = 5;

    private int cycles = 1000;

    private int faults = 0;

    static class Tuple {
        String key;
        int value;
        Proposal proposal;

        Tuple(String key, Proposal proposal) {
            this.key = key;
            this.proposal = proposal;
        }

        Tuple(String key, Proposal proposal, int value) {
            this(key, proposal);
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s(%d:%d)", key, proposal.id, proposal.tc);
        }
    }

    class Proposal {

        long structure;

        int tb;
        int tp;
        int tc;
        int id = txnIdCounter++;

        String message;

        List<Tuple> writeSet;
        List<Tuple> readSet;
        List<Tuple> writeSet0;
        List<Tuple> readSet0;
        Set<Proposal> inConflicts;
        Set<Proposal> outConflicts;

        private int sf(int field, int a, int b) {
            return (int) (((structure >>> field) % (b - a)) + a);
        }

        private boolean isSpecialSkew() {
            return sf(16, 0, 100) < PERCENT_SPECIAL_SKEW;
        }

        @Override
        public String toString() {
            return String.format("id=%4d tb=%4d tp=%4d tc=%4d reads=%s writes=%s  %s", id, tb, tp, tc, readSet,
                    writeSet, message == null ? "" : message);
        }
    }

    TxnModel(int seed) {
        random = new Random(seed);
    }

    private String randomVariable(List<Tuple> others, int seed) {
        boolean done = false;
        String variable = null;
        int v = seed;
        while (!done) {
            variable = new String(new char[] { (char) (65 + (v % 20)) });
            v = (v * v + 13) & Integer.MAX_VALUE;
            done = true;
            for (Tuple tuple : others) {
                if (variable.equals(tuple.key)) {
                    done = false;
                    break;
                }
            }
        }
        return variable;
    }

    private void create(Proposal proposal) {
        proposal.tb = ++timestampCounter;
        proposal.structure = random.nextLong() & Long.MAX_VALUE;
    }

    private void execute(Proposal proposal) {
        proposal.readSet0 = proposal.readSet;
        proposal.writeSet0 = proposal.writeSet;
        proposal.readSet = new ArrayList<Tuple>();
        proposal.writeSet = new ArrayList<Tuple>();
        proposal.inConflicts = new HashSet<Proposal>();
        proposal.outConflicts = new HashSet<Proposal>();

        int reads = proposal.sf(0, 0, readSize);
        int writes = proposal.sf(8, 0, writeSize);
        boolean specialSkew = proposal.sf(16, 0, 100) < PERCENT_SPECIAL_SKEW;
        int readVarSeed = proposal.sf(32, 0, 65536);
        int writeVarSeed = proposal.sf(48, 0, 65536);
        int value = proposal.sf(24, 0, 256);

        for (int index = 0; index < reads; index++) {
            String variable = randomVariable(proposal.readSet, readVarSeed);
            final Tuple tuple = get(variable, proposal.tb);
            proposal.readSet.add(tuple);
            value = hash(value, tuple);
        }
        for (int index = 0; index < writes; index++) {
            String variable = randomVariable(proposal.writeSet, writeVarSeed);
            value = value + variable.hashCode();
            proposal.writeSet.add(new Tuple(variable, proposal, value));
        }
        if (specialSkew) {
            Tuple ytuple = get("Y", proposal.tb);
            proposal.readSet.add(ytuple);
            Tuple ztuple = get("Z", proposal.tb);
            proposal.readSet.add(ztuple);
            value = hash(value, ytuple);
            value = hash(value, ztuple);
            String variable = proposal.sf(60, 0, 2) == 0 ? "Y" : "Z";
            int v = ytuple.value + ztuple.value + value;
            if (v < 0) {
                value -= v;
            }
            proposal.writeSet.add(new Tuple(variable, proposal, value));
        }

        if (proposal.readSet0 != null) {
            compareReadSets(proposal, proposal.readSet0, proposal.readSet);
        }

        for (final Tuple tuple : proposal.writeSet) {
            List<Tuple> dbtuples = database.get(tuple.key);
            if (dbtuples == null) {
                dbtuples = new ArrayList<Tuple>();
                database.put(tuple.key, dbtuples);
            }
            dbtuples.add(tuple);
        }
    }

    private void compareReadSets(Proposal proposal, List<Tuple> a, List<Tuple> b) {
        if (a.size() != b.size()) {
            System.out.printf("Read sets for %s are not the same length\n", proposal);
            faults++;
        }
        for (int i = 0; i < a.size(); i++) {
            Tuple atuple = a.get(i);
            Tuple btuple = b.get(i);
            assert atuple.key.equals(btuple.key);
            if (atuple.proposal.tc != btuple.proposal.tc) {
                System.out.printf("Different read timestamps: a=%s b=%s\n", atuple, btuple);
            }
        }
    }

    private int hash(int value, Tuple tuple) {
        return (short) ((value * 17) ^ (tuple.key.hashCode()) ^ (tuple.value * 13));
    }

    private void run() {
        List<Proposal> proposals = new ArrayList<Proposal>();
        allProposals = new Proposal[cycles + 1];
        allProposals[0] = PRIMORDIAL_PROPOSAL;
        int aborts = 0;
        int writeSkews = 0;
        for (int index = 0;;) {
            int r = random.nextInt(concurrency + proposals.size());
            if (index < cycles && r < concurrency) {
                Proposal proposal = new Proposal();
                create(proposal);
                execute(proposal);
                proposals.add(proposal);
                allProposals[proposal.id] = proposal;
                index++;
            } else if (index >= cycles && proposals.isEmpty()) {
                break;
            } else if (r >= concurrency) {
                Proposal proposal = proposals.remove(r - concurrency);
                proposal.tp = ++timestampCounter;
                if (commit(proposal)) {
                    if (proposal.isSpecialSkew()) {
                        Tuple y = get("Y", proposal.tc);
                        Tuple z = get("Z", proposal.tc);
                        if (y.value + z.value < 0) {
                            writeSkews++;
                            proposal.message = "WriteSkew";
                            faults++;
                        }
                    }
                    System.out.printf("%,5d commit %s\n", timestampCounter, proposal);

                    committedProposals.put(proposal.tc, proposal);

                } else {
                    proposal.tc = -1;
                    System.out.printf("%,5d  abort %s\n", timestampCounter, proposal);
                    aborts++;
                }
            }
        }
        TreeMap<String, Integer> copy = new TreeMap<String, Integer>(values);

        values.clear();
        database.clear();
        timestampCounter = 0;

        System.out.println();

        int index = 0;
        for (final Proposal proposal : committedProposals.values()) {
            if (!adjustCommitTime(proposal)) {
                faults++;
            }
            System.out.printf("%,5d  reexec %s\n", index, proposal);
            proposal.tb = proposal.tc;
            execute(proposal);
            put(proposal);
            index++;
        }

        int mismatches = 0;

        for (final Map.Entry<String, Integer> entry : values.entrySet()) {
            final Integer nonSerialValue = copy.get(entry.getKey());
            if (nonSerialValue == null || !nonSerialValue.equals(entry.getValue())) {
                // System.out.println(entry + " != " + nonSerialValue);
                mismatches++;
                faults++;
            }
        }
        System.out
                .printf("%d cycles, %d aborts, %d mismatches, %d writeSkews ", cycles, aborts, mismatches, writeSkews);
    }

    private boolean adjustCommitTime(Proposal proposal) {
        int tproposed = proposal.tc;
        for (Tuple tuple : proposal.readSet) {
            Tuple between = between(tuple.key, tuple.proposal.tc + 1, proposal.tc);
            if (between != null && between.proposal.tc < tproposed) {
                tproposed = between.proposal.tc;
            }
        }
        if (tproposed < proposal.tc) {
            for (Proposal other : committedProposals.values()) {
                if (other.tc > proposal.tc) {
                    break;
                }
                if (other != proposal && other.tc >= tproposed) {
                    for (Tuple t : proposal.writeSet) {
                        for (Tuple u : other.readSet) {
                            if (t.key.equals(u.key) && tproposed < u.proposal.tc) {
                                System.out.printf("Failed to adjust proposal %s TC %d --> %d due to %s\n", proposal,
                                        proposal.tc, tproposed, other);
                                faults++;
                                return false;
                            }
                        }
                    }
                }
            }
            proposal.message = String.format(" TC %d-->%d", proposal.tc, tproposed - 1);
            proposal.tc = tproposed - 1;
            if (tproposed <= proposal.tb){
                proposal.message += " (before tb=" + proposal.tb + ")";
            }
        }
        return true;
    }

    private Tuple get(String key, int timestamp) {
        List<Tuple> dbtuples = database.get(key);
        Tuple best = new Tuple(key, PRIMORDIAL_PROPOSAL);
        if (dbtuples != null) {
            for (final Tuple tuple : dbtuples) {
                if (tuple.proposal.tc > 0 && tuple.proposal.tc <= timestamp) {
                    if (best == null || tuple.proposal.tc > best.proposal.tc) {
                        best = tuple;
                    }
                }
            }
        }
        return best;
    }

    private Tuple between(String key, int from, int to) {
        Tuple result = null;
        List<Tuple> dbtuples = database.get(key);
        if (dbtuples != null) {
            for (Tuple tuple : dbtuples) {
                if (tuple.proposal.tc >= from && tuple.proposal.tc <= to) {
                    if (result == null || (tuple.proposal.tc < result.proposal.tc)) {
                        result = tuple;
                    }
                }
            }
        }
        return result;
    }

    private void put(Proposal proposal) {
        for (final Tuple tuple : proposal.writeSet) {
            values.put(tuple.key, tuple.value);
        }
    }

    private boolean commit(final Proposal proposal) {
        int tproposed;

        tproposed = proposal.tp;
        // First-Committer-Wins
        for (Tuple tuple : proposal.writeSet) {
            Tuple between = between(tuple.key, proposal.tb, Integer.MAX_VALUE);
            if (between != null) {
                proposal.message = "FCW(" + between + ")";
                return false;
            }
        }

        proposal.tc = tproposed;
        
//        if (!adjustCommitTime(proposal)) {
//            faults++;
//        }

        if (!rwDependencies(proposal)) {
            return false;
        }

        if (!rwDependencies2(proposal)) {
            return false;
        }

        put(proposal);
        return true;
    }

    private boolean rwDependencies(Proposal proposal) {
        // Cahill/Fekete - dangerous structure detection
        // Scan read set for rw-dependencies. Look for other
        // concurrent committed transactions having overlapping
        // write sets that overlap this proposal's read set.
        Set<Proposal> rwDependencies = new HashSet<Proposal>();
        for (Tuple tuple : proposal.readSet) {
            List<Tuple> dbtuples = database.get(tuple.key);
            if (dbtuples != null) {
                for (Tuple t : dbtuples) {
                    if (t.proposal.tc >= proposal.tb && tuple.proposal.tc <= proposal.tc) {
                        rwDependencies.add(t.proposal);
                    }
                }
            }
        }

        for (final Proposal other : rwDependencies) {
            proposal.outConflicts.add(other);
            other.inConflicts.add(proposal);
        }

        if (proposal.inConflicts.isEmpty()) {
            for (final Proposal potentialPivot : rwDependencies) {
                if (!potentialPivot.outConflicts.isEmpty()) {
                    proposal.message = "Pivot=" + potentialPivot;
                    return false;
                }
            }
        } else if (!proposal.outConflicts.isEmpty()) {
            proposal.message = "Pivot=<this>";
            return false;
        }
        return true;
    }

    private boolean rwDependencies2(Proposal proposal) {
        // Scan write set for newly created rw-dependencies
        // This loop looks at the read-sets of already-committed transactions
        // and adds an rw-dependency from any such transaction to the current
        // proposal if the current proposal is concurrent and has an overlapping
        // write set.
        for (Proposal other : committedProposals.values()) {
            if (other.tc > proposal.tb && !proposal.inConflicts.contains(other)) {
                // other proposal is concurrent with this one.
                for (Tuple t : proposal.writeSet) {
                    for (Tuple u : other.readSet) {
                        if (u.key.equals(t.key)) {
                            other.outConflicts.add(proposal);
                            proposal.inConflicts.add(other);
                            if (!proposal.outConflicts.isEmpty()) {
                                proposal.message = "Pivot2=<this>";
                                return false;
                            }
                            if (!other.inConflicts.isEmpty()) {
                                proposal.message = "Pivot2=" + other;
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    public static void main(final String[] args) {
        int cyclesPerSeed = 100;
        int concurrency = 15;
        int readSize = 3;
        int writeSize = 3;
        int seeds = 1;

        if (args.length > 0) {
            cyclesPerSeed = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            concurrency = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            readSize = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            writeSize = Integer.parseInt(args[3]);
        }
        if (args.length > 4) {
            seeds = Integer.parseInt(args[4]);
        }

        for (int seed = 1; seed <= seeds; seed++) {
            TxnModel model = new TxnModel(seed);
            model.cycles = cyclesPerSeed;
            model.concurrency = concurrency;
            model.readSize = readSize;
            model.writeSize = writeSize;

            System.out.printf("\n\nSeed= %d\n\n", seed);

            model.run();

            if (model.faults > 0) {
                break;
            }
        }
    }
}
