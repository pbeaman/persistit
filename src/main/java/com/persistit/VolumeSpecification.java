/**
 * 
 */
package com.persistit;

import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.persistit.exception.InvalidVolumeSpecificationException;

public class VolumeSpecification {

    private final static String ATTR_ALIAS = "alias";
    private final static String ATTR_CREATE = "create";
    private final static String ATTR_READONLY = "readOnly";
    private final static String ATTR_CREATEONLY = "createOnly";
    private final static String ATTR_TRANSIENT = "transient";
    private final static String ATTR_LOOSE = "loose";
    private final static String ATTR_PAGE_SIZE = "pageSize";
    private final static String ATTR_PAGE2_SIZE = "bufferSize";
    private final static String ATTR_ID = "id";

    private final static String ATTR_INITIAL_SIZE = "initialSize";
    private final static String ATTR_EXTENSION_SIZE = "extensionSize";
    private final static String ATTR_MAXIMUM_SIZE = "maximumSize";

    private final static String ATTR_INITIAL_PAGES = "initialPages";
    private final static String ATTR_EXTENSION_PAGES = "extensionPages";
    private final static String ATTR_MAXIMUM_PAGES = "maximumPages";

    private String path;
    private String name = null;
    private boolean readOnly = false;
    private boolean create = false;
    private boolean createOnly = false;
    private boolean tranzient = false;
    private boolean loose = false;
    private int bufferSize = 8192;
    private long id = 0;
    private long initialPages = -1;
    private long extensionPages = -1;
    private long maximumPages = -1;
    private long initialSize = -1;
    private long extensionSize = -1;
    private long maximumSize = -1;

    public VolumeSpecification(final String specification)
            throws InvalidVolumeSpecificationException {
        StringTokenizer mainTokenizer = new StringTokenizer(specification, ",");
        try {
            path = mainTokenizer.nextToken().trim();

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
                } else if (ATTR_TRANSIENT.equals(attr)) {
                    tranzient = true;
                } else if (ATTR_LOOSE.equals(attr)) {
                    loose = true;
                } else if (ATTR_ALIAS.equals(attr)) {
                    String valueString = innerTokenizer.nextToken().trim();
                    if (valueString != null && valueString.length() > 0) {
                        name = valueString;
                    }
                } else {
                    String valueString = innerTokenizer.nextToken().trim();
                    boolean bad = false;
                    long value = Persistit.parseLongProperty(attr, valueString,
                            0, Long.MAX_VALUE);

                    if (ATTR_PAGE_SIZE.equals(attr)
                            || ATTR_PAGE2_SIZE.equals(attr)) {
                        bufferSize = (value > Integer.MAX_VALUE) ? Integer.MAX_VALUE
                                : (int) value;
                    } else if (ATTR_ID.equals(attr)) {
                        id = value;
                    } else if (ATTR_INITIAL_PAGES.equals(attr)) {
                        initialPages = value;
                    } else if (ATTR_EXTENSION_PAGES.equals(attr)) {
                        extensionPages = value;
                    } else if (ATTR_MAXIMUM_PAGES.equals(attr)) {
                        maximumPages = value;
                    } else if (ATTR_INITIAL_SIZE.equals(attr)) {
                        initialSize = value;
                    } else if (ATTR_EXTENSION_SIZE.equals(attr)) {
                        extensionSize = value;
                    } else if (ATTR_MAXIMUM_SIZE.equals(attr)) {
                        maximumSize = value;
                    } else
                        bad = true;
                    if (bad || innerTokenizer.hasMoreTokens()) {
                        throw new InvalidVolumeSpecificationException(
                                "Unknown attribute " + attr + " in "
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
                throw new InvalidVolumeSpecificationException(specification
                        + ": readOnly, create and createOnly "
                        + "attributes are mutually exclusive");
            }
            if (readOnly && tranzient) {
                throw new InvalidVolumeSpecificationException(specification
                        + ": readOnly and transient attributes "
                        + "are mutually exclusive");
            }
            //
            // Allows size specification in bytes rather than pages.
            //
            if (bufferSize > 0) {
                if (initialPages == -1 && initialSize > 0) {
                    initialPages = (initialSize + (bufferSize - 1))
                            / bufferSize;
                }
                if (extensionPages == -1 && extensionSize > 0) {
                    extensionPages = (extensionSize + (bufferSize - 1))
                            / bufferSize;
                }
                if (maximumPages == -1 && maximumSize > 0) {
                    maximumPages = (maximumSize + (bufferSize - 1))
                            / bufferSize;
                }
            }
        } catch (NumberFormatException nfe) {
            throw new InvalidVolumeSpecificationException(specification
                    + ": invalid number");
        } catch (NoSuchElementException nste) {
            throw new InvalidVolumeSpecificationException(specification + ": "
                    + nste);
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

    public boolean isTransient() {
        return tranzient;
    }

    public boolean isLoose() {
        return loose;
    }

    public int getBufferSize() {
        return bufferSize;
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

    public String describe() {
        if (name != null) {
            return name;
        } else {
            return path;
        }
    }

}