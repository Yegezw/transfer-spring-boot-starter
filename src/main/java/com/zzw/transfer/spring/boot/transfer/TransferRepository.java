package com.zzw.transfer.spring.boot.transfer;

import com.google.common.base.Stopwatch;
import com.zzw.transfer.spring.boot.listener.SimpleTransferEventPublisher;
import com.zzw.transfer.spring.boot.listener.TransferListener;
import com.zzw.transfer.spring.boot.listener.TransferStartEvent;
import com.zzw.transfer.spring.boot.listener.TransferStopEvent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TransferRepository
{

    private static final double BASE = 1_000_000_000.0;

    private final Map<Object, Transfer<?, ?>>        transfer;
    private final Map<Object, Long>                  counter;
    private final Map<Object, LinkedList<Stopwatch>> stopwatch;
    private final SimpleTransferEventPublisher       publisher;

    public TransferRepository(List<Transfer<?, ?>> transferList, List<TransferListener> listeners)
    {
        transfer  = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        counter   = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        stopwatch = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        for (final Transfer<?, ?> t : transferList)
        {
            Object mark = t.getMark();
            transfer.put(mark, t);
            counter.put(mark, 0L);
            stopwatch.put(mark, new LinkedList<>());
        }
        publisher = new SimpleTransferEventPublisher();
        publisher.addTransferListener(listeners);
    }

    public Transfer<?, ?> get(Object mark)
    {
        return transfer.get(mark);
    }

    // ------------------------------------------------

    public void updateCount(Bucket bucket)
    {
        Object mark = bucket.getMark();
        counter.put(mark, counter.get(mark) + bucket.getHandledData().size());
    }

    public long getCount(Bucket bucket)
    {
        return counter.get(bucket.getMark());
    }

    public void resetCount(Bucket bucket)
    {
        Object mark = bucket.getMark();
        counter.put(mark, 0L);
    }

    // ------------------------------------------------

    public void start(Object mark)
    {
        publisher.publishEvent(new TransferStartEvent(mark));
        Stopwatch unstarted = Stopwatch.createUnstarted();
        stopwatch.get(mark).addLast(unstarted);
        unstarted.start();
    }

    public double stop(Bucket bucket)
    {
        Object mark    = bucket.getMark();
        long   elapsed = stopwatch.get(mark).removeFirst().stop().elapsed(TimeUnit.NANOSECONDS);
        publisher.publishEvent(new TransferStopEvent(mark));
        return elapsed / BASE;
    }
}
