package com.persistit.newtxn;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Sketch of an object that could sit at the root of a TransactionIndex hash table bucket.
 * @author peter
 *
 */
public class TxnIndexBucket {

    long floor;
    TxnStatus current;
    TxnStatus aborted;
    TxnStatus long_running;
    TxnStatus free;
    ReentrantLock lock;
    
}
