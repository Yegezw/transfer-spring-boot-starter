package com.zzw.transfer.spring.boot.transfer;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("all")
public class Dispatcher
{

    protected final ImmutableMap<Object, Transfer> immutableMap;

    public Dispatcher(List<Transfer> transferList)
    {
        Map<Object, Transfer> map = new HashMap<>((int) (transferList.size() / 0.75 + 1));
        for (final Transfer transfer : transferList)
        {
            map.put(transfer.getMark(), transfer);
        }
        this.immutableMap = ImmutableMap.copyOf(map);
    }

    public Transfer getTransfer(Object mark)
    {
        return immutableMap.get(mark);
    }
}
