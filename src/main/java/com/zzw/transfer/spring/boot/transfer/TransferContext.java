package com.zzw.transfer.spring.boot.transfer;

public class TransferContext
{

    private static final double BASE = 1_000_000_000.0;

    private Object mark;
    private Object startupParam;
    private int    count;

    private long startTime;
    private long endTime;
    private long elapsed;

    public TransferContext()
    {
    }

    public Object getMark()
    {
        return mark;
    }

    public void setMark(Object mark)
    {
        this.mark = mark;
    }

    public Object getStartupParam()
    {
        return startupParam;
    }

    public void setStartupParam(Object startupParam)
    {
        this.startupParam = startupParam;
    }

    public int getCount()
    {
        return count;
    }

    public void setCount(int count)
    {
        this.count = count;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public void setStartTime(long startTime)
    {
        this.startTime = startTime;
    }

    public long getEndTime()
    {
        return endTime;
    }

    public void setEndTime(long endTime)
    {
        this.endTime = endTime;
        setElapsed();
    }

    public long getElapsed()
    {
        return elapsed;
    }

    public double getElapsedSecond()
    {
        return elapsed / BASE;
    }

    private void setElapsed()
    {
        elapsed = endTime - startTime;
    }
}
