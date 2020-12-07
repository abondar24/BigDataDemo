package org.abondar.experimental.mapreducedemo.data;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public IntPair() {
    }


    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }


    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }


    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compareTo(IntPair pair) {
        int cmp = compare(first, pair.first);
        if (cmp != 0) {
            return cmp;
        }
        return compare(second, pair.second);
    }

    public static int compare(int a, int b) {

        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntPair pair = (IntPair) o;

        if (first != pair.first) return false;
        return second == pair.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public String toString() {
        return "IntPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
