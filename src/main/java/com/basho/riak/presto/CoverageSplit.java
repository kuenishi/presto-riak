package com.basho.riak.presto;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;

import java.util.List;

/**
 * Created by kuenishi on 14/03/28.
 */
public class CoverageSplit implements Split{

    public CoverageSplit(SplitTask splitTask)
    {
        //TODO: fill here, encode splitTask as String
        splitTask.toString();
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public Object getInfo() {
        return null;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return null;
    }
}
