package com.zzw.transfer.spring.boot.transfer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HttpIterator<E> implements Iterator<E>
{

    private Iterator<E> i;

    @Override
    public boolean hasNext()
    {
        if (i != null && i.hasNext())
        {
            return true;
        }

        List<E> data = fetchData();
        if (data.isEmpty()) return false;

        i = data.iterator();
        return i.hasNext();
    }

    @Override
    public E next()
    {
        return i.next();
    }

    private List<E> fetchData()
    {
        // HTTP 获取数据
        // TODO 多线程 ?
        return new ArrayList<>();
    }
}
