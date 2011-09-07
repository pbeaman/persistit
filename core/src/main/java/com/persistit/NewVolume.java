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
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.exception.PersistitException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeNotFoundException;

/**
 * Represent the identity and optionally the service classes that manage a
 * volume.
 * 
 * @author peter
 */
public class NewVolume {

    private final String _name;
    private final long _id;

    private final AtomicInteger _handle = new AtomicInteger();
    private final AtomicReference<Object> _appCache = new AtomicReference<Object>();

    private NewVolumeSpecification _specification;
    private NewVolumeStorage _storage;
    private NewVolumeStatistics _statistics;
    private NewVolumeStructure _structure;

    public static boolean isValidPageSize(final int pageSize) {
        for (int b = 1024; b <= 16384; b *= 2) {
            if (b == pageSize) {
                return true;
            }
        }
        return false;
    }

    public NewVolume(final String name, final long id) {
        _name = name;
        _id = id;
    }

    public NewVolume(final NewVolumeSpecification specification) {
        this(specification.getName(), specification.getId());
        _specification = specification;
    }

    NewVolumeSpecification getSpecification() {
        return _specification;
    }

    NewVolumeStorage getStorage() {
        return _storage;
    }

    NewVolumeStatistics getStatistics() {
        return _statistics;
    }

    NewVolumeStructure getStructure() {
        return _structure;
    }

    /**
     * Open an existing Volume file or create a new one, depending on the
     * settings of the {@link NewVolumeSpecification}.
     * 
     * @throws PersistitException
     */
    public void open(final Persistit persistit) throws PersistitException {
        if (_specification == null) {
            throw new IllegalStateException("Missing VolumeSpecification");
        }
        if (_storage != null) {
            throw new IllegalStateException("This volume has already been opened");
        }
        final boolean exists = NewVolumeHeader.detectVersion(_specification);
        _structure = new NewVolumeStructure(persistit, this);
        _storage = new NewVolumeStorage(persistit, this);
        
        if (exists) {
            if (_specification.isCreateOnly()) {
                throw new VolumeAlreadyExistsException(_specification.getPath());
            }
            _storage.open();
        } else {
            if (!_specification.isCreate()) {
                throw new VolumeNotFoundException(_specification.getPath());
            }
            _storage.create();
        }
    }

    public String getName() {
        return _name;
    }

    public long getId() {
        return _id;
    }

    /**
     * Store an Object with this Volume for the convenience of an application.
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
