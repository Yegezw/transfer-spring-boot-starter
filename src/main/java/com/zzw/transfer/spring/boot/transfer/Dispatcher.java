package com.zzw.transfer.spring.boot.transfer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class Dispatcher
{

    private static final double BASE = 1_000_000_000.0;

    protected final ImmutableMap<Object, Transfer> immutableMap;
    protected final Map<Object, Long>              counter;
    protected final Map<Object, Stopwatch>         stopwatch;

    public Dispatcher(List<Transfer> transferList)
    {
        Map<Object, Transfer> map = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        counter   = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        stopwatch = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        for (final Transfer transfer : transferList)
        {
            Object mark = transfer.getMark();
            map.put(mark, transfer);
            counter.put(mark, 0L);
            stopwatch.put(mark, Stopwatch.createUnstarted());
        }
        this.immutableMap = ImmutableMap.copyOf(map);
    }

    public Transfer getTransfer(Object mark)
    {
        return immutableMap.get(mark);
    }

    public void updateCount(Bucket bucket)
    {
        Object mark = bucket.getMark();
        counter.put(mark, counter.get(mark) + bucket.getData().size());
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

    public void start(Object mark)
    {
        stopwatch.get(mark).reset().start();
    }

    public double stop(Bucket bucket)
    {
        Object mark    = bucket.getMark();
        long   elapsed = stopwatch.get(mark).elapsed(TimeUnit.NANOSECONDS);
        return elapsed / BASE;
    }
}
