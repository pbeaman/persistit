package com.persistit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages locks by thread.
 * 
 * @author peter
 * 
 */

public class LockManager {
	
    private final ThreadLocal<ResourceTracker> _resourceThreadLocal = new ThreadLocal<ResourceTracker>();

    private final Map<Thread, ResourceTracker> _resourceTrackerMap = new ConcurrentHashMap<Thread, ResourceTracker>();
    
    static class ResourceTracker {
        private final List<SharedResource> _resources =
            new ArrayList<SharedResource>();
        private int _offset;
        private boolean _disabled;

        @Override
		public String toString() {
            return "[" + _offset + "]"
                + _resources;
        }

        private void register(final SharedResource resource) {
            if (!_disabled) {
                _resources.add(resource);
            }
        }

        private void unregister(final SharedResource resource) {
            if (!_disabled) {
            final int size = _resources.size();
            final int position = size - _offset - 1;
            final SharedResource matchingResource = _resources.get(position);
            Debug.debug1(matchingResource != resource);
            _resources.remove(position);
            _offset = 0;
            }
        }
        
        private boolean isMine(final SharedResource resource) {
            for (int index = _resources.size(); --index >= 0;) {
                if (_resources.get(index) == resource) {
                    return true;
                }
            }
            return false;
        }

        private int getLockedResourceCount() {
            return _resources.size();
        }

        private boolean verifyLockedResourceCount(final int count) {
            if (!_disabled && count != _resources.size()) {
                Debug.debug1(true);
                return false;
            }
            return true;
        }

        private void setOffset(final int offset) {
            _offset = offset;
        }
        
        private void setDisabled(final boolean disabled) {
            _disabled = disabled;
        }
        
        private boolean isDisabled() {
            return _disabled;
        }
    }

    private ResourceTracker getMyResourceTracker() {
    	ResourceTracker tracker = _resourceThreadLocal.get();
    	if (tracker == null) {
            tracker = new ResourceTracker();
            _resourceTrackerMap.put(Thread.currentThread(), tracker);
            _resourceThreadLocal.set(tracker);
        }
        return tracker;
    }

    void register(final SharedResource resource) {
        getMyResourceTracker().register(resource);
    }

    void unregister(final SharedResource resource) {
        getMyResourceTracker().unregister(resource);
    }
    
    public boolean isMine(final SharedResource resource) {
        return getMyResourceTracker().isMine(resource);
    }

    public int getLockedResourceCount() {
        return getMyResourceTracker().getLockedResourceCount();
    }

    public boolean verifyNoStrayResourceClaims(final int count) {
        return getMyResourceTracker().verifyLockedResourceCount(count);
    }

    public void setOffset() {
        getMyResourceTracker().setOffset(1);
    }
    
    public void setOffset(final int offset) {
        getMyResourceTracker().setOffset(offset);        
    }
    
    public void setDisabled(final boolean disabled) {
        getMyResourceTracker().setDisabled(disabled);
    }
    
    public boolean isDisabled() {
        return getMyResourceTracker().isDisabled();
    }

    @Override
    public synchronized String toString() {
        final SortedMap<Thread, ResourceTracker> sorted =
            new TreeMap<Thread, ResourceTracker>(_resourceTrackerMap);
        final StringBuilder sb = new StringBuilder(500);
        for (final Map.Entry<Thread, ResourceTracker> entry : sorted.entrySet()) {
            sb.append(entry.getKey());
            sb.append("-->");
            sb.append(entry.getValue());
            sb.append(Persistit.NEW_LINE);
        }
        return sb.toString();
    }

}
