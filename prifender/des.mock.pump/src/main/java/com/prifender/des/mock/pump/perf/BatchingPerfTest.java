package com.prifender.des.mock.pump.perf;

public abstract class BatchingPerfTest extends PerfTest
{
    @Override
    public String label()
    {
        return name() + " [Batch Size " + batchSize() + "]";
    }

}
