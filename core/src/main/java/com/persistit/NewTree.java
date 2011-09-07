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
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * Represents a single B-Tree within a {@link NewVolume}.
 * 
 * @version 1.1
 */
public class NewTree extends SharedResource {
    private final String _name;
    private final NewVolume _volume;
    private volatile long _rootPageAddr;
    private volatile int _depth;
    private AtomicLong _changeCount = new AtomicLong(-1);
    int _hashCode = -1;
    private AtomicReference<Object> _appCache = new AtomicReference<Object>();
    private AtomicInteger _handle = new AtomicInteger();

    NewTree(final Persistit persistit, NewVolume volume, String name) {
        super(persistit);
        _name = name;
        _volume = volume;
    }

    public NewVolume getVolume() {
        return _volume;
    }

    public String getName() {
        return _name;
    }

    // TODO - renameTree
    @Override
    public int hashCode() {
        if (_hashCode < 0) {
            _hashCode = (_volume.hashCode() ^ _name.hashCode()) & 0x7FFFFFFF;
        }
        return _hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NewTree) {
            NewTree tree = (NewTree) o;
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
        return _rootPageAddr;
    }

    /**
     * Returns the number of levels of the <code>Tree</code>.
     * 
     * @return The depth
     */
    public int getDepth() {
        return _depth;
    }

    void changeRootPageAddr(long rootPageAddr, int deltaDepth) {
        Debug.$assert0.t(isMine());
        _rootPageAddr = rootPageAddr;
        _depth += deltaDepth;
        setDirty();
    }

    /**
     * Atomically returns the current depth and current root page address in a
     * Exchange.LevelCache. Used intimately by Exchange. This is done in here
     * because the _lock object is protected.
     * 
     * @param lc
     */
    void loadRootLevelInfo(Exchange exchange) {
        exchange.setRootLevelInfo(_rootPageAddr, _depth, getGeneration());
    }

    void bumpChangeCount() {
        //
        // Note: the changeCount only gets written when there's a structure
        // change in the tree that causes it to be committed.
        //
        _changeCount.incrementAndGet();
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
            throw new IllegalStateException("Invalid tree name recorded: " + name + " for tree " + _name);
        }
        _rootPageAddr = Util.getLong(encoded, 0);
        _depth = Util.getShort(encoded, 12);
        _changeCount.set(Util.getLong(encoded, 16));
        setValid();
    }

    /**
     * Initialize a Tree.
     * 
     * @param rootPageAddr
     * @throws PersistitException
     */
    void init(final long rootPageAddr) throws PersistitException {
        _rootPageAddr = rootPageAddr;
        setValid();
        setDirty();
        // Derive the index depth
        Buffer buffer = null;
        try {
            buffer = getVolume().getStructure().getPool().get(_volume, rootPageAddr, false, true);
            int type = buffer.getPageType();
            _depth = type - Buffer.PAGE_TYPE_DATA + 1;
        } finally {
            if (buffer != null)
                buffer.releaseTouched();
        }
    }

    /**
     * Invoked when this <code>Tree</code> is being deleted. This causes
     * subsequent operations by any <code>Exchange</code>s on this
     * <code>Tree</code> to fail.
     */
    void invalidate() {
        super.clearValid();
        _generation.set(-1);
    }

    /**
     * Returns a displayable description of the <code>Tree</code>, including its
     * name, its internal tree index, its root page address, and its depth.
     * 
     * @return A displayable summary
     */
    @Override
    public String toString() {
        return "<Tree " + _name + " rootPageAddr=" + _rootPageAddr + " depth=" + _depth + " status="
                + getStatusDisplayString() + ">";
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

    /**
     * @return The handle value used to identify this Tree in the journal
     */
    public int getHandle() {
        return _handle.get();
    }

    /**
     * Set the handle used to identify this Tree in the journal. May be invoked
     * only once.
     * 
     * @param handle
     * @return
     * @throws IllegalStateException
     *             if the handle has already been set
     */
    int setHandle(final int handle) {
        if (!_handle.compareAndSet(0, handle)) {
            throw new IllegalStateException("Tree handle already set");
        }
        return handle;
    }

}
