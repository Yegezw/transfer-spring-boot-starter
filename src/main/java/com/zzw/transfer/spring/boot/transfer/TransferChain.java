package com.zzw.transfer.spring.boot.transfer;

import com.zzw.transfer.spring.boot.listener.TransferEvent;
import com.zzw.transfer.spring.boot.listener.TransferListener;
import com.zzw.transfer.spring.boot.listener.TransferStartEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class TransferChain implements TransferListener
{

    private static final Logger log = LoggerFactory.getLogger(TransferChain.class);

    private final AtomicBoolean started = new AtomicBoolean(false);

    private TransferNode head;
    private TransferNode tail;

    private TransferNode curr;
    private Thread       leader;

    public TransferChain(List<Transfer<?, ?>> transferList)
    {
        for (Transfer<?, ?> transfer : transferList)
        {
            add(transfer);
        }
    }

    public void add(Transfer<?, ?> transfer)
    {
        if (tail == null)
        {
            curr = head = tail = new TransferNode(transfer);
        }
        else
        {
            tail = tail.next = new TransferNode(transfer);
        }
    }

    public void start()
    {
        if (!started.compareAndSet(false, true))
        {
            throw new RuntimeException("TransferLeader 已启动, 不可重复启动");
        }

        leader = Thread.currentThread();
        leader.setName("数据同步-单线程发布");
        while (curr != null)
        {
            boolean success = curr.transfer.start();
            if (success)
            {
                curr = curr.next;
                LockSupport.park(this);
            }
            else
            {
                Object mark = curr.transfer.getMark();
                log.error("{} 启动失败, 链条终止", mark);
                break;
            }
        }

        reset();
        started.set(false);
    }

    @Override
    public void onTransferEvent(TransferEvent event)
    {
        if (event instanceof TransferStartEvent) return;
        if (leader != null)
        {
            LockSupport.unpark(leader);
        }
    }

    private void reset()
    {
        leader = null;
        curr   = head;
    }

    private static class TransferNode
    {
        Transfer<?, ?> transfer;
        TransferNode   next;

        public TransferNode(Transfer<?, ?> transfer)
        {
            this.transfer = transfer;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb  = new StringBuilder();
        TransferNode  cur = head;
        while (cur != null)
        {
            sb.append(cur.transfer).append("\n");
            cur = cur.next;
        }
        return sb.toString();
    }
}
