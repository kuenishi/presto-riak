package com.basho.riak.presto;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.ericsson.otp.erlang.*;

import java.util.*;
import static com.google.common.base.Preconditions.*;


/**
 * Created by kuenishi on 14/03/29.
 */
// @doc translated from internal riak_object.
// direct translation.
public class RiakObject implements IRiakObject {
    private final byte[] key;
    private final byte[] bucket;
    private List<byte[]> values;

    public RiakObject(OtpErlangObject object)
    {
        checkRecord(object, "r_object");

        OtpErlangTuple rObject = (OtpErlangTuple)object;
//        System.out.println(rObject);
//        -record(r_object, {
//                bucket :: bucket(),
        this.bucket = ((OtpErlangBinary)rObject.elementAt(1)).binaryValue();
//                key :: key(),
        this.key = ((OtpErlangBinary)rObject.elementAt(2)).binaryValue();
//                contents :: [#r_content{}],
        OtpErlangList contents = (OtpErlangList)rObject.elementAt(3);
        values = new ArrayList();
        for(OtpErlangObject content : contents)
        {
            checkRecord(content, "r_content");
//            -record(r_content, {
//                    metadata :: dict() | list(),
//                    value :: term()
            OtpErlangBinary b = (OtpErlangBinary)((OtpErlangTuple)content).elementAt(2);
            values.add(b.binaryValue());
//            }).
        }
//        vclock = vclock:fresh() :: vclock:vclock(),
//            updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
//            updatevalue :: term()
//        }).
    }

    private void checkRecord(OtpErlangObject o, String v)
    {
        checkAtom(((OtpErlangTuple)o).elementAt(0), v);
    }
    private void checkAtom(OtpErlangObject o, String v)
    {
        checkState(((OtpErlangAtom)o).atomValue().equals(v));
    }

    @Override
    public void setValue(byte[] bytes) {
    }

    @Override
    public void setValue(String s) {

    }

    @Override
    public Iterator<RiakLink> iterator() {
        return null;
    }

    @Override
    public Iterable<Map.Entry<String, String>> userMetaEntries() {
        return null;
    }

    @Override
    public IRiakObject addUsermeta(String s, String s2) {
        return null;
    }

    @Override
    public IRiakObject addLink(RiakLink riakLink) {
        return null;
    }

    @Override
    public IRiakObject addIndex(String s, long l) {
        return null;
    }

    @Override
    public IRiakObject addIndex(String s, String s2) {
        return null;
    }

    @Override
    public VClock getVClock() {
        return null;
    }

    @Override
    public Set<String> getBinIndex(String s) {
        return null;
    }

    @Override
    public String getKey() {
        return new String(key);
    }

    @Override
    public String getBucket() {
        return new String(bucket);
    }

    @Override
    public String getUsermeta(String s) {
        return null;
    }

    @Override
    public Map<IntIndex, Set<Integer>> allIntIndexes() {
        return null;
    }

    @Override
    public Map<IntIndex, Set<Long>> allIntIndexesV2() {
        return null;
    }

    @Override
    public Map<BinIndex, Set<String>> allBinIndexes() {
        return null;
    }

    @Override
    public IRiakObject removeUsermeta(String s) {
        return null;
    }

    @Override
    public IRiakObject removeIntIndex(String s) {
        return null;
    }

    @Override
    public IRiakObject removeBinIndex(String s) {
        return null;
    }

    @Override
    public boolean hasUsermeta(String s) {
        return false;
    }

    @Override
    public boolean hasUsermeta() {
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean hasLinks() {
        return false;
    }

    @Override
    public boolean hasLink(RiakLink riakLink) {
        return false;
    }

    @Override
    public int numLinks() {
        return 0;
    }

    @Override
    public String getVtag() {
        return null;
    }

    @Override
    public Set<Long> getIntIndexV2(String s) {
        return null;
    }

    @Override
    public IRiakObject removeLink(RiakLink riakLink) {
        return null;
    }

    @Override
    public String getContentType() {
        return null;
    }

    @Override
    public Set<Integer> getIntIndex(String s) {
        return null;
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public void setContentType(String s) {

    }

    @Override
    public byte[] getValue() {
//        if(values.size()==1)
        return values.get(0);
    }

    @Override
    public Date getLastModified() {
        return null;
    }

    @Override
    public List<RiakLink> getLinks() {
        return null;
    }

    @Override
    public String getValueAsString() {
        return new String(getValue());
    }

    @Override
    public String getVClockAsString() {
        return null;
    }

    @Override
    public Map<String, String> getMeta() {
        return null;
    }

}
