package com.persistit;

import com.persistit.JournalManager.VolumeDescriptor;

interface VolumeHandleLookup {

    VolumeDescriptor lookupVolumeHandle(final int handle);
}