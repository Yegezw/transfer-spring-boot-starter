package com.zzw.transfer.spring.boot.transfer;

import com.zzw.transfer.spring.boot.listener.TransferEvent;
import com.zzw.transfer.spring.boot.listener.TransferListener;
import com.zzw.transfer.spring.boot.listener.TransferStopEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class TransferChain implements TransferListener
{

    private static final Logger log = LoggerFactory.getLogger(TransferChain.class);

    private final AtomicBoolean started = new AtomicBoolean(false);

    // ------------------------------------------------

    private final String chinaName;

    private TransferNode head;
    private TransferNode tail;

    private TransferNode cur;
    private Thread       thread;

    public TransferChain(String chinaName)
    {
        this.chinaName = chinaName;
    }

    public TransferChain(String chinaName, List<Transfer<?, ?>> transferList)
    {
        this.chinaName = chinaName;
        for (Transfer<?, ?> transfer : transferList)
        {
            add(transfer);
        }
    }

    // ------------------------------------------------

    public TransferChain add(Transfer<?, ?> transfer)
    {
        if (tail == null)
        {
            head = tail = new TransferNode(transfer);
        }
        else
        {
            tail = tail.next = new TransferNode(transfer);
        }
        return this;
    }

    /**
     * 线程安全的启动
     *
     * @return {@code true} 该链的每个转移器 {@link Transfer#start(Object)} 都启动成功, 且完成入库
     * <br>{@code false} 该链有任意一个转移器 {@link Transfer#start(Object)} 启动失败
     */
    public boolean start(Object startupParam)
    {
        if (!started.compareAndSet(false, true))
        {
            throw new RuntimeException("TransferLeader 已启动, 不可重复启动");
        }

        thread = Thread.currentThread();
        String oldName = thread.getName();
        thread.setName("数据同步-单线程发布-" + chinaName);

        boolean succeed = true;

        cur = head;
        while (cur != null)
        {
            boolean success = cur.start(startupParam);
            if (success)
            {
                LockSupport.park(this);
                cur = cur.next;
            }
            else
            {
                Object mark = cur.getMark();
                succeed = false;
                log.error("{} 启动失败, 链条终止", mark);
                break;
            }
        }

        thread.setName(oldName);
        thread = null;
        cur    = null;
        started.set(false);
        return succeed;
    }

    @Override
    public void onTransferEvent(TransferEvent event)
    {
        if (event instanceof TransferStopEvent)
        {
            if (thread != null && event.getMark() == cur.getMark())
            {
                LockSupport.unpark(thread);
            }
        }
    }

    // ------------------------------------------------

    private static class TransferNode
    {
        Transfer<?, ?> transfer;
        TransferNode   next;

        public TransferNode(Transfer<?, ?> transfer)
        {
            this.transfer = transfer;
        }

        public Object getMark()
        {
            return transfer.getMark();
        }

        public boolean start(Object startupParam)
        {
            return transfer.start(startupParam);
        }

        @Override
        public String toString()
        {
            return transfer.toString();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(chinaName).append("\n");
        TransferNode cur = head;
        while (cur != null)
        {
            sb.append(cur).append("\n");
            cur = cur.next;
        }
        return sb.toString();
    }
}
