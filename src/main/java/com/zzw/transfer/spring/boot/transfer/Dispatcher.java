package com.zzw.transfer.spring.boot.transfer;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("all")
public class Dispatcher
{

    protected final ImmutableMap<Object, Transfer> immutableMap;
    protected final Map<Object, Long>              counter;

    public Dispatcher(List<Transfer> transferList)
    {
        Map<Object, Transfer> map = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        counter = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        for (final Transfer transfer : transferList)
        {
            map.put(transfer.getMark(), transfer);
            counter.put(transfer.getMark(), 0L);
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
}
