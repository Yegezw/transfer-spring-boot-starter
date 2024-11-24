package com.zzw.transfer.spring.boot.transfer;

import java.util.List;

@SuppressWarnings("all")
public class Bucket
{

    private List   data = null;
    private Object mark = null;

    public Bucket()
    {
    }

    void clear()
    {
        data = null;
        mark = null;
    }

    public List getData()
    {
        return data;
    }

    public void setData(List data)
    {
        this.data = data;
    }

    public Object getMark()
    {
        return mark;
    }

    public void setMark(Object mark)
    {
        this.mark = mark;
    }
}
