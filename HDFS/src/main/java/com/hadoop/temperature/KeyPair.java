package com.hadoop.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by LDDFY on 2017/1/7.
 */
public class KeyPair implements WritableComparable<KeyPair> {
    private int year;
    private int hot;

    public KeyPair() {
    }

    public KeyPair(int year, int hot) {
        this.year = year;
        this.hot = hot;
    }

    public int getYear() {

        return year;
    }

    public void setYear(int year) {

        this.year = year;
    }

    public int getHot() {

        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    @Override
    public int hashCode() {
        return new Integer(year + hot).hashCode();
    }

    public int compareTo(KeyPair o) {
        int res = Integer.compare(year, o.getYear());
        if (res == 0) {
            return res;
        }
        return Integer.compare(hot, o.getHot());
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(year);
        output.writeInt(hot);
    }

    public void readFields(DataInput input) throws IOException {
        this.year = input.readInt();
        this.hot = input.readInt();
    }

    @Override
    public String toString() {
        return "KeyPair{" +
                "year=" + year +
                ", hot=" + hot +
                '}';
    }
}
