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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

public class Debug
{
    public final static boolean ENABLED = false;

    public final static boolean VERIFY_PAGES = false;
    
    public final static boolean HISTORY_ENABLED = false;
    
    public final static boolean IOLOG_ENABLED = false;
    

    private static int _suspendedCount;
    // Lazily instantiated
    private static Random _random;
    
    private static HashMap _threadClaims = new HashMap();
    
    private static ArrayList _brokenThreads = new ArrayList();
    
    private static long _startTime;

    public static void setStartTime(final long startTime) {
    	_startTime = startTime;
    }
    
    public static long elapsedTime() {
    	return now() - _startTime;
    }
    
    public static long now() {
    	return System.currentTimeMillis();
    }
    
    private static void logDebugMessage(String msg)
    {
        long now = now();

        DebugException de = new DebugException();
        de.fillInStackTrace();
        String s = LogBase.detailString(de).replace('\r', ' ');
        StringTokenizer st = new StringTokenizer(s, "\n");
        StringBuffer sb = new StringBuffer(msg);
        sb.append("\r\n");
        while (st.hasMoreTokens())
        {
            sb.append("    ");
            sb.append(st.nextToken());
            sb.append("\r\n");
        }
        System.err.println("Debug " + sb.toString());
    }
    
    private static class DebugException
    extends Exception
    {
    }
    
    /**
     * Use this method for a conditional breakpoint that executes at full
     * speed.  Set a debugger breakpoint where indicated. 
     * @param condition <i>true</i> if the breakpoint should be taken
     * @return  <i>true</i>
     */
    public static boolean debug(boolean condition)
    {
        if (!condition) return false;
        // Put a breakpoint on this return statement.
        logDebugMessage("debug");    
        return true;               // <-- BREAKPOINT HERE
    }

    /**
     * Use this method for a conditional breakpoint that executes at full
     * speed.  Set a debugger breakpoint where indicated.  This method also
     * sets the suspend flag so other threads will be suspended at a suspend
     * point if necessary.  (Simplifies debugging because the diagnostic UI
     * still works in this situation.) 
     * @param condition <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug0(boolean condition)
    {
        if (condition)
        {
            logDebugMessage("debug0");    
            setSuspended(true);
            long time = elapsedTime();

            //    
            // Put a breakpoint on the next statement.
            //
            setSuspended(false);        // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Use this method for a conditional breakpoint that executes at full
     * speed.  Set a debugger breakpoint where indicated.  This method also
     * sets the suspend flag so other threads will be suspended at a suspend
     * point if necessary.  (Simplifies debugging because the diagnostic UI
     * still works in this situation.) 
     * @param condition <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug1(boolean condition)
    {
        if (condition)
        {
            logDebugMessage("debug1");    
            setSuspended(true);
            long time = elapsedTime();
            //    
            // Put a breakpoint on the next statement.
            //
            setSuspended(false);        // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }


    /**
     * Use this method for a conditional breakpoint that executes at full
     * speed.  Set a debugger breakpoint where indicated.  This method also
     * sets the suspend flag so other threads will be suspended at a suspend
     * point if necessary.  (Simplifies debugging because the diagnostic UI
     * still works in this situation.)
     * @param condition <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug2(boolean condition)
    {
        if (condition)
        {
            logDebugMessage("debug2");    
            setSuspended(true);
            long time = elapsedTime();
            
            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false);        // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Use this method for a conditional breakpoint that executes at full
     * speed.  Set a debugger breakpoint where indicated.  This method also
     * sets the suspend flag so other threads will be suspended at a suspend
     * point if necessary.  (Simplifies debugging because the diagnostic UI
     * still works in this situation.) 
     * @param condition <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug3(boolean condition)
    {
        if (condition)
        {
            logDebugMessage("debug3");    
            setSuspended(true);
            long time = elapsedTime();
            
            //    
            // Put a breakpoint on the next statement.
            //
            setSuspended(false);        // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Replace assert statements with calls to this method in order to
     * take a breakpoint before throwing the AssertionError if the condition
     * is false.
     * @param condition
     */
    public static void $assert(boolean condition)
    {
        if (!condition)
        {
            setSuspended(true);
            logDebugMessage("$assert");
            long time = elapsedTime();
            
            //    
            // Put a breakpoint on the next statement.
            //
            setSuspended(false);        // <-- BREAKPOINT HERE
            /*JDK14*///assert(false);
        }
    }

    /**
     * Invoke this method to sleep briefly on a random basis.  This method
     * invokes sleep approximately once per thousand invocations.
     */
    public static void debugPause()
    {
        debugPause(true);
    }
    
    /**
     * Invoke this method to sleep briefly on a random basis.  This method
     * invokes sleep approximately once per thousand invocations on which the
     * condition is true.
     * @param condition <i>true<i> to whether to pause with 0.1% probability
     */
    public static void debugPause(boolean condition)
    {
        debugPause(condition, 0.001);
    }
    
    
    /**
     * Invoke this method to sleep briefly on a random basis.  Supply a
     * double value to indicate the probability that should be used.
     * @param condition <i>true<i> to whether to pause
     */
    public static void debugPause(boolean condition, double probability)
    {
        if (_random == null) _random = new Random(1000);
        if (condition && probability >= 1.0 ||
            _random.nextFloat() < probability)
        {
            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException ie)
            {
            }
        }
    }
    
    /**
     * Invoke this method to perform a System.exit() operation on a random
     * basis according to the supplied probability.
     * @param probability
     */
    public static void debugExit(double probability)
    {
        if (_random == null) _random = new Random(1000);
        if (probability >= 1.0 ||
            _random.nextFloat() < probability)
        {
            logDebugMessage("debugExit");    
            System.out.println();
            System.out.println("DEBUG EXIT!");
            System.exit(0);
        }
    }
    
    /**
     * Set the suspend flag so that callers to the suspend method either do
     * or do not suspend.
     * @param b
     */
    synchronized static void setSuspended(boolean b)
    {
        if (b)
        {
            _suspendedCount++;
            _brokenThreads.add(Thread.currentThread());
        }
        else
        {
            _suspendedCount--;
            _brokenThreads.remove(Thread.currentThread());
            if (_suspendedCount == 0)
            {
                $assert(_brokenThreads.size() == _suspendedCount);
            }
        }
    }
    
    /**
     * @return  The state of the suspend flag.
     */
    synchronized static boolean isSuspended()
    {
        return _suspendedCount > 0;
    }
    
    /**
     * Assert this method invocation anywhere you want to suspend a thread.
     * For example, add this to cause execution to be suspended:
     * 
     *      assert(Debug.suspend());
     * 
     * This method always returns true so there will never be an AssertionError
     * thrown.
     * @return  <i>true</i>
     */
    static boolean suspend()
    {
        // Never suspend the AWT thread when.  The AWT thread is now
        // a daemon thread when running the diagnostic GUI utility.
        //
        long time = -1;
        while (isSuspended() && !Thread.currentThread().isDaemon())
        {
            if (time < 0) time = elapsedTime();
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException ie)
            {
            }
        }
        return true;
    }
    
    static Debug initDebugHistory()
    {
        if (IOLOG_ENABLED) _ih = new IOHistory();
        else _ih = null;
        
        if (HISTORY_ENABLED) _dh = new DebugHistory();
        else _dh = null;
        
        return new Debug();
    }
    // -------------
    // Debugging platform for watching page state transitions.
    //
    private static DebugHistory _dh;
    private static IOHistory _ih;
    
    // Dor convenience in the debugger - don't need to enable static variables.
    private DebugHistory _debugHistory = _dh;
    private IOHistory _ioHistory = _ih;
    
    static class DebugHistory
    {
        private DebugPageHistory[] _debugHistory = new DebugPageHistory[100000];
        DebugHistory()
        {
            for (int index = 0; index < _debugHistory.length; index++)
            {
                _debugHistory[index] = new DebugPageHistory(index == 0 ? 20 : 20);
            }
        }
    }
    
    static class DebugPageEvent
    {
        long _logTime;
        String _eventName;
        long _pwjbGeneration;
        long _bufferGeneration;
        long _changeCount;
        int _bufferType;
        int _poolIndex;
        Thread _writerThread;
        int _status;
        Thread _thread;
    
        @Override
		public String toString()
        {
            if (_thread == null) return "<empty>";
        
            StringBuffer sb = new StringBuffer();
            sb.append(_eventName);
            while (sb.length() < 15)
            {
                sb.append(" ");
            }
            Util.fill(sb, _logTime, 8);
            sb.append(" pwjbGeneration=");
            Util.fill(sb, _pwjbGeneration, 5);
            sb.append(" changeCount=");
            sb.append(_changeCount);
            sb.append(" type=");
            sb.append(_bufferType);
            sb.append(" Thread=<");
            sb.append(_thread.getName());
            sb.append(">");
            if (_poolIndex >= 0)
            {
                sb.append(" @");
                sb.append(_poolIndex);
                sb.append(" ");
                sb.append(SharedResource.getStatusCode(_status));
                if (_writerThread != null)
                {
                    sb.append("<");
                    sb.append(_writerThread.getName());
                    sb.append(">");
                }
            }            
            return sb.toString();
        }
    }

    static class DebugPageHistory
    {
        int _next = 0;
        DebugPageEvent[] _events;
        long _changeCount = 0;
    
        DebugPageHistory(int size)
        {
            _events = new DebugPageEvent[size];
            for (int index = 0; index < size; index++)
            {
                _events[index] = new DebugPageEvent();
            }
        }
    
        @Override
		public String toString()
        {
            StringBuffer sb = new StringBuffer("\r\n");
            int start = _next - _events.length + 1;
            if (start < 0) start = 0;
            for (int i = start; i < _next; i++)
            {
                int index = (i + _events.length) % _events.length;
                Util.fill(sb, i, 5);
                sb.append(" ");
                sb.append(_events[index]);
                sb.append("\r\n");
            }
            return sb.toString();
        }
        
    }
    
    static DebugPageEvent stateChanged(
        Volume volume,
        long pageAddress, 
        int bufferType,
        String eventName,
        long changeCount,
        long generation)
    {
        if (_dh == null) return null;
        
        // TODO !!
        String volumeName = volume.getPathName();
        if (volumeName.indexOf("sys") >= 0 || volumeName.indexOf("txn") >= 0)
        {
            return null;
        }
        
        int page = (int)pageAddress;
        if (page < _dh._debugHistory.length)
        {
            DebugPageHistory dph = _dh._debugHistory[page];
            DebugPageEvent dpe;
            int size = dph._events.length;
            synchronized(dph)
            {
                int index = (dph._next + size) % size; 
                dpe = dph._events[index];
                dph._next++;
            }
            dpe._eventName = eventName;
            dpe._pwjbGeneration = generation;
            dpe._changeCount = changeCount;
            dpe._bufferType = bufferType;
            dpe._thread = Thread.currentThread();
            dpe._logTime = elapsedTime();
            // Obliterate these fields when not relevant
            dpe._poolIndex = -1;
            dpe._bufferGeneration = -1;
            dpe._status = 0;
            dpe._writerThread = null;

            if (eventName.startsWith("init"))
            {
                dph._changeCount = 0;
            }
            else if (!eventName.startsWith("write"))
            {
                boolean sequenceFailure =
                    "dirty".equals(eventName) 
                        ? changeCount <= dph._changeCount
                        : changeCount < dph._changeCount;
                        
                if (sequenceFailure)
                {
                    logDebugMessage(
                        "Sequence failure: change count for page " + page + 
                        " is " + changeCount + 
                        " but should be " + dph._changeCount + 
                        " for " + eventName);
                        
                    logDebugMessage(dph.toString());
                }
                dph._changeCount = changeCount;
            }
            
            return dpe;
        }
        else
        {
            return null;
        }
    }

    static DebugPageEvent stateChanged(
        Buffer buffer,
        String eventName,
        long generation)
    {
        if (_dh == null) return null;
    
        DebugPageEvent dpe =
            stateChanged(
                buffer.getVolume(),
                buffer.getPageAddress(),
                buffer.getPageType(),
                eventName,
                buffer.getTimestamp(),
                generation);
        if (dpe != null)
        {
            dpe._poolIndex = buffer.getIndex();
            dpe._status = buffer.getStatus();
            dpe._writerThread = buffer.getWriterThread();
            dpe._bufferGeneration = buffer.getTimestamp();
        }
        return dpe;
    }
    
    static IOLogEvent startIOEvent(String type, long item)
    {
        if (IOLOG_ENABLED && _ih != null) return _ih.startIOEvent(type, item);
        else return null;
    }
    
    static void endIOEvent(IOLogEvent event, Exception ex)
    {
        if (IOLOG_ENABLED && _ih != null) _ih.endIOEvent(event, ex);
    }

    static class IOHistory
    {
    
        IOLogEvent[] _ioLogBuffer = new IOLogEvent[IOLOG_ENABLED ? 25000 : 0];
        int _ioLogCounter = 0;
        
        synchronized IOLogEvent startIOEvent(String type, long item)
        {
            if (IOLOG_ENABLED)
            {
                int id = _ioLogCounter++;
                int index = id % _ioLogBuffer.length;
                IOLogEvent event = _ioLogBuffer[index];
                if (event == null)
                {
                    _ioLogBuffer[index] = event = new IOLogEvent();
                }
                event._id = id;
                event._item = item;
                event._type = type;
                event._endTime = 0;
                event._startTime = System.currentTimeMillis();
                event._ex = null;
                event._thread = Thread.currentThread();
                return event;
            }
            else return null;
        }
        
        void endIOEvent(IOLogEvent event, Exception exception)
        {
            if (event != null)
            {
                event._endTime = System.currentTimeMillis();
                event._ex = exception;
            }
        }
        
        @Override
		public String toString()
        {
            StringBuffer sb = new StringBuffer();
            int end = _ioLogCounter;
            int start = end - _ioLogBuffer.length;
            if (start < 0) start = 0;
            for (int counter = start; counter < end; counter++)
            {
                int index = counter % _ioLogBuffer.length;
                sb.append("  ");
                sb.append(_ioLogBuffer[index]);
                sb.append("\r\n");
            }
            return sb.toString();
        }
    }
    
    static class IOLogEvent
    {
        int _id;
        long _item;
        String _type;
        long _startTime;
        long _endTime;
        Exception _ex;
        Thread _thread;
        
        @Override
		public String toString()
        {
            StringBuffer sb = new StringBuffer();
            Util.fill(sb, _id, 5);
            sb.append(" ");
            Util.fill(sb, _type, 15);
            Util.fill(sb, _item, 6);
            sb.append(" start=");
            Util.fill(sb, _startTime - Debug._startTime, 8);
            sb.append(" elapsed=");
            if (_endTime <= 0) sb.append("  incomplete");
            else Util.fill(sb, _endTime - _startTime, 12);
            if (_ex != null)
            {
                sb.append(" ");
                sb.append(_ex);
            }
            return sb.toString();
        }
    }
    
    static void registerClaimMap(HashMap map)   //TODO - remove
    {
        synchronized(_threadClaims)
        {
            _threadClaims.put(Thread.currentThread(), map);
        }
    }
    
    static void dump(Throwable t)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileName = "Persistit_Dump_" + sdf.format(new Date()) + ".txt";
        try
        {
            PrintWriter pw =
                new PrintWriter(new FileOutputStream(
                    new File(fileName)));
            dump(pw, t);
        }
        catch (IOException ioe)
        {
            dump(new PrintWriter(System.out), t);
        }
    }
    
    static void dump(PrintWriter pw, Throwable t)
    {
        try
        {
            pw.println("------------------");
            pw.println(
                "DUMP at time=" + 
                System.currentTimeMillis() + 
                " elapsed=" + elapsedTime());
            pw.println("------------------");
            pw.println();
            if (t != null)
            {
                t.printStackTrace(pw);
                pw.println();
            }
            if (_ih != null)
            {
                
                int end = _ih._ioLogCounter;
                int start = end - _ih._ioLogBuffer.length;
                if (start < 0) start = 0;
                
                pw.println("------------------");
                pw.println("IOLog: start=" + start + " end=" + end);
                pw.println("------------------");

                pw.println();
                
                for (int counter = start; counter < end; counter++)
                {
                    int index = counter % _ih._ioLogBuffer.length;
                    pw.println(_ih._ioLogBuffer[index]);
                }
            }
            pw.println();
            pw.println("------------------");
            pw.println("OUTSTANDING CLAIMS");
            pw.println("------------------");
            HashMap claims = null;
            synchronized(_threadClaims)
            {
                claims = new HashMap(_threadClaims);
            }
            
            for (Iterator iter = claims.entrySet().iterator();
                 iter.hasNext(); )
            {
                Map.Entry entry = (Map.Entry)iter.next();
                Thread th = (Thread)entry.getKey();
                HashMap map = (HashMap)entry.getValue();
                if (!map.isEmpty())
                {
                    pw.println(
                        "  Thread " + th.getName() + 
                        " (toString=" + th.toString() + 
                        (th.isAlive() ? ") ALIVE" : ")DEAD") +
                        (th.isDaemon() ? " DAEMON" : ""));
                    
                    for (Iterator iter2 = map.keySet().iterator();
                         iter2.hasNext(); )
                    {
                        Object whatsIt = iter2.next();
                        SharedResource sr = (SharedResource)whatsIt;
                        pw.println("    " + sr);
                        if (sr instanceof Buffer)
                        {
                            Buffer buffer = (Buffer)sr;
                            long page = buffer.getPageAddress();
                            if (_dh != null)
                            {
                                DebugPageHistory[] dphArray = _dh._debugHistory;
                                if (dphArray != null && page < dphArray.length)
                                {
                                    DebugPageHistory dph = dphArray[(int)page];
                                    pw.println(dph);
                                }
                            }
                        }
                    }
                    pw.println();
                }
            }
            pw.println();
            pw.println("------------------");
            pw.println("DUMP FINISHED");
            pw.println("------------------");

        }
        finally
        {
            pw.flush();
            pw.close();
        }
    }
    
}
