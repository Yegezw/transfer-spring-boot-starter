package com.zzw.transfer.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "transfer")
public class TransferProperties
{

    public static final String TRANSFER_PREFIX = "transfer";


    private static final boolean MULTI_THREAD_WRITE = true;
    private static final int     HANDLER_THREAD_NUM = 1;
    private static final int     SAVER_THREAD_NUM   = 10;
    private static final int     BUFFER_SIZE        = 512;


    /**
     * 缓冲区大小, 必须是 2 的幂次
     */
    private int bufferSize = BUFFER_SIZE;

    /**
     * 是否多线程写入
     */
    private boolean multiThreadWrite = MULTI_THREAD_WRITE;

    /**
     * 处理线程数
     */
    private int handlerThreadNum = HANDLER_THREAD_NUM;

    /**
     * 保存线程数
     */
    private int saverThreadNum = SAVER_THREAD_NUM;


    public int getBufferSize()
    {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize)
    {
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        this.bufferSize = bufferSize;
    }

    public boolean isMultiThreadWrite()
    {
        return multiThreadWrite;
    }

    public void setMultiThreadWrite(boolean multiThreadWrite)
    {
        this.multiThreadWrite = multiThreadWrite;
    }

    public int getHandlerThreadNum()
    {
        return handlerThreadNum;
    }

    public void setHandlerThreadNum(int handlerThreadNum)
    {
        this.handlerThreadNum = handlerThreadNum;
    }

    public int getSaverThreadNum()
    {
        return saverThreadNum;
    }

    public void setSaverThreadNum(int saverThreadNum)
    {
        this.saverThreadNum = saverThreadNum;
    }
}
