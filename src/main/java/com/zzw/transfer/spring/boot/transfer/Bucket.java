package com.zzw.transfer.spring.boot.transfer;

import java.util.List;

@SuppressWarnings("rawtypes")
public class Bucket
{

    private List    data        = null;
    private List    handledData = null;
    private Object  mark        = null;
    private boolean lastPublish = false;

    public Bucket()
    {
    }

    public void clear()
    {
        data        = null;
        handledData = null;
        mark        = null;
        lastPublish = false;
    }

    public List getData()
    {
        return data;
    }

    public void setData(List data)
    {
        this.data = data;
    }

    public List getHandledData()
    {
        return handledData;
    }

    public void setHandledData(List handledData)
    {
        this.handledData = handledData;
    }

    public Object getMark()
    {
        return mark;
    }

    public void setMark(Object mark)
    {
        this.mark = mark;
    }

    public boolean isLastPublish()
    {
        return lastPublish;
    }

    public void setLastPublish(boolean lastPublish)
    {
        this.lastPublish = lastPublish;
    }
}
