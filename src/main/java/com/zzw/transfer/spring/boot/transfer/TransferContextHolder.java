package com.zzw.transfer.spring.boot.transfer;

public class TransferContextHolder
{

    private static final ThreadLocal<TransferContext> currentContext = new ThreadLocal<>();

    private TransferContextHolder()
    {
    }

    public static TransferContext currentContext()
    {
        TransferContext context = currentContext.get();
        if (context == null)
        {
            throw new RuntimeException("上下文不存在");
        }
        return context;
    }

    public static TransferContext setCurrentContext(TransferContext context)
    {
        TransferContext old = currentContext.get();
        if (context != null)
        {
            currentContext.set(context);
        }
        else
        {
            currentContext.remove();
        }
        return old;
    }
}
