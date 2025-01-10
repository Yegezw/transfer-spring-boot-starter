package com.zzw.transfer.spring.boot.transfer;

import java.util.List;

public abstract class DefaultTransfer<S, T> extends Transfer<S, T>
{

    private static final int DEFAULT_BUCKET_SIZE = 2000;

    // ------------------------------------------------

    @Override
    protected int getBucketSize()
    {
        return DEFAULT_BUCKET_SIZE;
    }

    @Override
    public void postProcessBeforeStart(Object startupParam)
    {
    }

    @Override
    public void postProcessAfterStart(Object startupParam)
    {
    }

    // ------------------------------------------------

    @Override
    protected boolean shouldHandle()
    {
        return false;
    }

    @Override
    protected List<T> doHandle(S source, boolean lastData)
    {
        return null;
    }

    @Override
    protected Object getHandleErrorInfo(S source, Exception e)
    {
        return null;
    }

    // ------------------------------------------------

    @Override
    protected boolean shouldCollectAfterHandle()
    {
        return false;
    }

    @Override
    protected List<T> doCollect(T target, boolean lastData)
    {
        return null;
    }
}
