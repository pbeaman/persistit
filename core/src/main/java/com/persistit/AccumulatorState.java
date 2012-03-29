/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;


/**
 * State of an accumulator recovered from a checkpoint.
 * 
 * @author peter
 */
final class AccumulatorState {

    String _treeName;
    int _index;
    Accumulator.Type _type;
    long _value;

    /**
     * Deserialize stored statistics values from the supplied byte array
     * starting at <code>index</code>.
     * 
     * @param bytes
     *            serialized statistics
     * @param index
     *            at which serialized statistics start in the byte array
     */
    void load(final Value value) {
        _treeName = value.getString();
        _index = value.getInt();
        _type = Accumulator.Type.valueOf(value.getString());
        _value = value.getLong();
    }

    @Override
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s value=%,d)", _treeName, _index, _type, _value);
    }
    
    public String getTreeName() {
        return _treeName;
    }
    
    public int getIndex() {
        return _index;
    }
    
    public Accumulator.Type getType() {
        return _type;
    }
    
    public long getValue() {
        return _value;
    }
}
