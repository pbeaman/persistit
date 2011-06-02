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

package com.persistit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private AtomicLong _changeCount = new AtomicLong(-1);
    int _hashCode = -1;
    private AtomicReference<Object> _appCache = new AtomicReference<Object>();
    private AtomicInteger _handle = new AtomicInteger();

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
     * Returns the page address of the root page of this <code>Tree</code>. The
     * root page will be a data page if the <code>Tree</code> has only one page,
     * or will be the top index page of the B-Tree.
     * 
     * @return The page address
     */
    public long getRootPageAddr() {
        synchronized (_lock) {
            return _rootPageAddr;
        }
    }

    /**
     * Returns the number of levels of the <code>Tree</code>.
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
        _changeCount.incrementAndGet();
    }

    /**
     * Sets the tree name to null. This is done only by Volume.getTree() which
     * may create a Tree and then immediately discard it. Clearing the change
     * count prevents removing the surviving tree having the same name from the
     * tree map.
     */
    void destroy() {
        _changeCount.set(-1);
    }

    long getChangeCount() {
        return _changeCount.get();
    }

    /**
     * Save a Tree in the directory
     * 
     * @param value
     */
    void store(Value value) {
        byte[] nameBytes = Util.stringToBytes(_name);
        byte[] encoded = new byte[32 + nameBytes.length];

        Util.putLong(encoded, 0, _rootPageAddr);
        Util.putShort(encoded, 12, _depth);
        Util.putLong(encoded, 16, getChangeCount());
        // 24-30 free
        Util.putShort(encoded, 30, nameBytes.length);
        Util.putBytes(encoded, 32, nameBytes);
        value.put(encoded);
    }

    /**
     * Load an existing Tree from the directory
     * 
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
        _changeCount.set(Util.getLong(encoded, 16));
        setValid(true);
    }

    /**
     * Initialize a Tree.
     * 
     * @param rootPageAddr
     * @throws PersistitException
     */
    void init(final long rootPageAddr) throws PersistitException {
        _rootPageAddr = rootPageAddr;
        setValid(true);
        setDirtyStructure();
        // Derive the index depth
        Buffer buffer = null;
        try {
            buffer = getVolume().getPool().get(getVolume(), rootPageAddr,
                    false, true);
            int type = buffer.getPageType();
            _depth = type - Buffer.PAGE_TYPE_DATA + 1;
        } finally {
            if (buffer != null)
                getVolume().getPool().release(buffer);
        }
    }

    /**
     * Invoked when this <code>Tree</code> is being deleted. This causes
     * subsequent operations by any <code>Exchange</code>s on this
     * <code>Tree</code> to fail.
     */
    void invalidate() {
        synchronized (_lock) {
            _generation = -1;
            super.setValid(false);
        }
    }

    /**
     * Returns a displayable description of the <code>Tree</code>, including its
     * name, its internal tree index, its root page address, and its depth.
     * 
     * @return A displayable summary
     */
    @Override
    public String toString() {
        return "<Tree " + _name + " rootPageAddr=" + _rootPageAddr + " depth="
                + _depth + " status=" + getStatusDisplayString() + ">";
    }

    /**
     * Store an Object with this Tree for the convenience of an application.
     * 
     * @param the
     *            object to be cached for application convenience.
     */
    public void setAppCache(Object appCache) {
        _appCache.set(appCache);
    }

    /**
     * @return the object cached for application convenience
     */
    public Object getAppCache() {
        return _appCache.get();
    }
    
    public int getHandle() {
        return _handle.get();
    }
    
    public int setHandle(final int handle) {
        _handle.set(handle);
        return handle;
    }


}
