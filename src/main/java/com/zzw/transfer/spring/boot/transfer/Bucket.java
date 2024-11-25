package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.util.Util;
import sun.misc.Unsafe;

import java.util.List;

@SuppressWarnings("all")
public class Bucket
{

    private List    data        = null;
    private Object  mark        = null;
    private boolean lastPublish = false;

    public Bucket()
    {
    }

    void clear()
    {
        data        = null;
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
        // 由 MESI 负责同步
        // 我不相信 MESI 的同步速度会比 MySQL 的写入速度还慢, 那真是太离谱了
        return lastPublish;
    }

    public void setLastPublish()
    {
        UNSAFE.putBooleanVolatile(this, LAST_PUBLISH_OFFSET, true);
    }

    private static final Unsafe UNSAFE;
    private static final long   LAST_PUBLISH_OFFSET;

    static
    {
        UNSAFE = Util.getUnsafe();
        try
        {
            LAST_PUBLISH_OFFSET = UNSAFE.objectFieldOffset(Bucket.class.getDeclaredField("lastPublish"));
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
