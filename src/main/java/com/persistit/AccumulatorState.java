/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
