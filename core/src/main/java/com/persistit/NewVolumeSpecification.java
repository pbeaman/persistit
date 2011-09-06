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

/**
 * 
 */
package com.persistit;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.persistit.exception.InvalidVolumeSpecificationException;

public class NewVolumeSpecification {

    private final static String ATTR_NAME = "name";
    private final static String ATTR_CREATE = "create";
    private final static String ATTR_READONLY = "readOnly";
    private final static String ATTR_CREATEONLY = "createOnly";
    private final static String ATTR_PAGE_SIZE = "pageSize";
    private final static String ATTR_ID = "id";

    private final static String ATTR_INITIAL_SIZE = "initialSize";
    private final static String ATTR_EXTENSION_SIZE = "extensionSize";
    private final static String ATTR_MAXIMUM_SIZE = "maximumSize";
    private final static String ATTR_VERSION = "version";

    private String path;
    private String name = null;
    private boolean readOnly = false;
    private boolean create = false;
    private boolean createOnly = false;

    private int pageSize = -1;
    private int version = -1;

    private long id = 0;
    private long initialPages = -1;
    private long extensionPages = -1;
    private long maximumPages = -1;
    private long initialSize = -1;
    private long extensionSize = -1;
    private long maximumSize = -1;

    public NewVolumeSpecification(final String path, final String name, final long id, final int pageSize,
            final long initialPages, final long maximumPages, final long extensionPages, final boolean create,
            final boolean createOnly, final boolean readOnly) {
        this.path = path;
        this.id = id;
        this.pageSize = pageSize;
        this.initialPages = initialPages;
        this.maximumPages = maximumPages;
        this.extensionPages = extensionPages;
        this.create = create;
        this.createOnly = createOnly;
        this.readOnly = readOnly;
    }

    public NewVolumeSpecification(final String specification) throws InvalidVolumeSpecificationException {
        StringTokenizer mainTokenizer = new StringTokenizer(specification, ",");
        try {
            path = mainTokenizer.nextToken().trim();
            name = new File(path).getName();

            while (mainTokenizer.hasMoreTokens()) {
                String token = mainTokenizer.nextToken().trim();
                StringTokenizer innerTokenizer = new StringTokenizer(token, ":");
                String attr = innerTokenizer.nextToken().trim();
                if (ATTR_READONLY.equals(attr)) {
                    readOnly = true;
                } else if (ATTR_CREATE.equals(attr)) {
                    create = true;
                } else if (ATTR_CREATEONLY.equals(attr)) {
                    createOnly = true;
                } else if (ATTR_NAME.equals(attr)) {
                    String valueString = innerTokenizer.nextToken().trim();
                    if (valueString != null && !valueString.isEmpty()) {
                        name = valueString;
                    }
                } else {
                    String valueString = innerTokenizer.nextToken().trim();
                    boolean bad = false;
                    long value = Persistit.parseLongProperty(attr, valueString, 0, Long.MAX_VALUE);

                    if (ATTR_PAGE_SIZE.equals(attr)) {
                        if (value < Integer.MAX_VALUE && value > 0 && NewVolume.isValidPageSize((int) value)) {
                            pageSize = (int) value;
                        } else {
                            throw new InvalidVolumeSpecificationException("Invalid pageSize " + specification);
                        }
                    } else if (ATTR_ID.equals(attr)) {
                        id = value;
                    } else if (ATTR_INITIAL_SIZE.equals(attr)) {
                        initialSize = value;
                    } else if (ATTR_EXTENSION_SIZE.equals(attr)) {
                        extensionSize = value;
                    } else if (ATTR_MAXIMUM_SIZE.equals(attr)) {
                        maximumSize = value;
                    } else {
                        bad = true;
                    }

                    if (bad || innerTokenizer.hasMoreTokens()) {
                        throw new InvalidVolumeSpecificationException("Unknown attribute " + attr + " in "
                                + specification);
                    }
                }
            }
            int n = 0;
            if (readOnly) {
                n++;
            }
            if (create) {
                n++;
            }
            if (createOnly) {
                n++;
            }
            if (n > 1) {
                throw new InvalidVolumeSpecificationException(specification + ": readOnly, create and createOnly "
                        + "attributes are mutually exclusive");
            }
            //
            // Allows size specification in bytes rather than pages.
            //
            if (pageSize > 0) {
                if (initialPages == -1 && initialSize > 0) {
                    initialPages = (initialSize + (pageSize - 1)) / pageSize;
                }
                if (extensionPages == -1 && extensionSize > 0) {
                    extensionPages = (extensionSize + (pageSize - 1)) / pageSize;
                }
                if (maximumPages == -1 && maximumSize > 0) {
                    maximumPages = (maximumSize + (pageSize - 1)) / pageSize;
                }
            }
            
            // Validate initial, maximum and extension sizes
            if (maximumPages == 0)
                maximumPages = initialPages;

            if (initialPages < 1 || initialPages > Long.MAX_VALUE / pageSize) {
                throw new InvalidVolumeSpecificationException("Invalid initial page count: " + initialPages);
            }

            if (extensionPages < 0 || extensionPages > Long.MAX_VALUE / pageSize) {
                throw new InvalidVolumeSpecificationException("Invalid extension page count: " + extensionPages);
            }

            if (maximumPages < initialPages || maximumPages > Long.MAX_VALUE / pageSize) {
                throw new InvalidVolumeSpecificationException("Invalid maximum page count: " + maximumPages);
            }
            
        } catch (NumberFormatException nfe) {
            throw new InvalidVolumeSpecificationException(specification + ": invalid number");
        } catch (NoSuchElementException nste) {
            throw new InvalidVolumeSpecificationException(specification + ": " + nste);
        }
    }

    public void setPageSize(final int value) throws InvalidVolumeSpecificationException {
        if (value > Integer.MAX_VALUE || value < 0 || !NewVolume.isValidPageSize((int) value)) {
            throw new InvalidVolumeSpecificationException("Invalid pageSize " + value);
        }

        if (pageSize == value || pageSize == -1) {
            pageSize = value;
        } else {
            throw new InvalidVolumeSpecificationException("Mismatched volume pageSize " + value + " for " + this);
        }
    }

    public void setVersion(final int value) throws InvalidVolumeSpecificationException {
        if (version == value || version == -1) {
            version = value;
        } else {
            throw new InvalidVolumeSpecificationException("Mismatched volume version " + value + " for " + this);
        }
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isCreate() {
        return create;
    }

    public boolean isCreateOnly() {
        return createOnly;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getId() {
        return id;
    }

    public long getInitialPages() {
        return initialPages;
    }

    public long getExtensionPages() {
        return extensionPages;
    }

    public long getMaximumPages() {
        return maximumPages;
    }

    public long getInitialSize() {
        return initialSize;
    }

    public long getExtensionSize() {
        return extensionSize;
    }

    public long getMaximumSize() {
        return maximumSize;
    }
    
    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(path);
        sb.append(',').append(ATTR_NAME).append(':').append(name);
        sb.append(',').append(ATTR_ID).append(':').append(id);
        sb.append(',').append(ATTR_PAGE_SIZE).append(':').append(pageSize);

        if (initialSize >= 0) {
            sb.append(',').append(ATTR_INITIAL_SIZE).append(':').append(ds(initialPages));
        }
        if (maximumSize >= 0) {
            sb.append(',').append(ATTR_MAXIMUM_SIZE).append(':').append(ds(maximumPages));
        }
        if (extensionSize >= 0) {
            sb.append(',').append(ATTR_EXTENSION_SIZE).append(':').append(ds(extensionPages));
        }
        if (readOnly) {
            sb.append(',').append(ATTR_READONLY);
        }
        if (createOnly) {
            sb.append(',').append(ATTR_CREATEONLY);
        } else if (create) {
            sb.append(',').append(ATTR_CREATE);
        }
        if (version != -1) {
            sb.append(',').append(ATTR_VERSION).append(':').append(version);
        }
        return sb.toString();
    }

    private String ds(final long pages) {
        return Persistit.displayableLongValue(pages * pageSize);
    }
    
    /**
     * @param initialPages the initialPages to set
     */
    void setInitialPages(long initialPages) {
        this.initialPages = initialPages;
    }

    /**
     * @param extensionPages the extensionPages to set
     */
    void setExtensionPages(long extensionPages) {
        this.extensionPages = extensionPages;
    }

    /**
     * @param maximumPages the maximumPages to set
     */
    void setMaximumPages(long maximumPages) {
        this.maximumPages = maximumPages;
    }



}