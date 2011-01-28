/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.persistit;

import com.persistit.exception.PersistitException;

/**
 * Represents a single B-Tree within a {@link Volume}.
 * 
 * @version 1.1
 */
public class Tree extends SharedResource {
    private final String _name;
    private final Volume _volume;
    private long _rootPageAddr;
    private int _depth;
    private long _changeCount;
    int _hashCode = -1;

    Tree(final Persistit persistit, Volume volume, String name) {
        super(persistit);
        _name = name;
        _volume = volume;
    }
    
    public Volume getVolume() {
        return _volume;
    }

    public String getName() {
        return _name;
    }

    @Override
    public int hashCode() {
        if (_hashCode < 0) {
            _hashCode = (_volume.hashCode() ^ _name.hashCode()) & 0x7FFFFFFF;
        }
        return _hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Tree) {
            Tree tree = (Tree) o;
            return _name.equals(tree._name) && _volume == tree.getVolume();
        } else
            return false;
    }

    /**
     * Returns the page address of the root page of this <tt>Tree</tt>. The root
     * page will be a data page if the <tt>Tree</tt> has only one page, or will
     * be the top index page of the B-Tree.
     * 
     * @return The page address
     */
    public long getRootPageAddr() {
        synchronized (_lock) {
            return _rootPageAddr;
        }
    }

    /**
     * Returns the number of levels of the <tt>Tree</tt>.
     * 
     * @return The depth
     */
    public int getDepth() {
        synchronized (_lock) {
            return _depth;
        }
    }

    void changeRootPageAddr(long rootPageAddr, int deltaDepth) {
        if (Debug.ENABLED)
            Debug.$assert(isMine());
        synchronized (_lock) {
            _rootPageAddr = rootPageAddr;
            _depth += deltaDepth;
            setDirty();
        }
    }

    /**
     * Atomically returns the current depth and current root page address in a
     * Exchange.LevelCache. Used intimately by Exchange. This is done in here
     * because the _lock object is protected.
     * 
     * @param lc
     */
    void loadRootLevelInfo(Exchange exchange) {
        synchronized (_lock) {
            exchange.setRootLevelInfo(_rootPageAddr, _depth, _generation);
        }
    }

    void commit() throws PersistitException {
        if (isDirty() && isValid()) {
            _volume.updateTree(this);
            setClean();
        }
    }

    void bumpChangeCount() {
        //
        // Note: the changeCount only gets written when there's a structure
        // change in the tree that causes it to be committed.
        //
        synchronized (_lock) {
            _changeCount++;
            setDirty();
        }
    }

    /**
     * Sets the tree name to null. This is done only by Volume.getTree() which
     * may create a Tree and then immediately discard it. Clearing the change count
     * prevents removing the surviving tree having the same name from the
     * tree map.
     */
    void destroy() {
        _changeCount = -1;
    }

    long getChangeCount() {
        synchronized (_lock) {
            return _changeCount;
        }
    }

    /**
     * Save a Tree in the directory
     * @param value
     */
    void store(Value value) {
        byte[] nameBytes = Util.stringToBytes(_name);
        byte[] encoded = new byte[32 + nameBytes.length];

        Util.putLong(encoded, 0, _rootPageAddr);
        Util.putShort(encoded, 12, _depth);
        Util.putLong(encoded, 16, _changeCount);
        // 24-30 free
        Util.putShort(encoded, 30, nameBytes.length);
        Util.putBytes(encoded, 32, nameBytes);
        value.put(encoded);
    }
    
    /**
     * Load an existing Tree from the directory
     * @param value
     */
    void load(Value value) {
        byte[] encoded = value.getByteArray();
        int nameLength = Util.getShort(encoded, 30);
        final String name = new String(encoded, 32, nameLength);
        if (!_name.equals(name)) {
            throw new IllegalStateException("Invalid tree name recorded: "
                    + name + " for tree " + _name);
        }
        _rootPageAddr = Util.getLong(encoded, 0);
        _depth = Util.getShort(encoded, 12);
        _changeCount = Util.getLong(encoded, 16);
    }

    /**
     * Initialize a Tree.
     * @param rootPageAddr
     * @throws PersistitException
     */
    void init(final long rootPageAddr) throws PersistitException {
        _rootPageAddr = rootPageAddr;
        setDirtyStructure();
        // Derive the index depth
        Buffer buffer = null;
        try {
            buffer = getVolume().getPool().get(getVolume(), rootPageAddr, false, true);
            int type = buffer.getPageType();
            _depth = type - Buffer.PAGE_TYPE_DATA + 1;
        } finally {
            if (buffer != null)
                getVolume().getPool().release(buffer);
        }
    }

    /**
     * Invoked when this <tt>Tree</tt> is being deleted. This causes subsequent
     * operations by any <tt>Exchange</tt>s on this <tt>Tree</tt> to fail.
     */
    void invalidate() {
        synchronized (_lock) {
            _generation = -1;
            super.setValid(false);
        }
    }

    /**
     * Returns a displayable description of the <tt>Tree</tt>, including its
     * name, its internal tree index, its root page address, and its depth.
     * 
     * @return A displayable summary
     */
    @Override
    public String toString() {
        return "<Tree " + _name + " rootPageAddr=" + _rootPageAddr + " depth="
                + _depth + " status=" + getStatusDisplayString() + ">";
    }

}
