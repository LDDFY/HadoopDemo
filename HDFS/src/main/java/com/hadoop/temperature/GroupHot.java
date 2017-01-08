package com.hadoop.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by LDDFY on 2017/1/7.
 */
public class GroupHot extends WritableComparator {

    public GroupHot() {
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair o1 = (KeyPair) a;
        KeyPair o2 = (KeyPair) b;
        return Integer.compare(o1.getYear(), o2.getYear());
    }
}
