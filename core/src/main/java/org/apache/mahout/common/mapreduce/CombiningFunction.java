package org.apache.mahout.common.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

public interface CombiningFunction<VALUE extends Writable> {
    public VALUE combine(VALUE value1, VALUE value2);

    static public class VectorWritableSum implements CombiningFunction<VectorWritable> {
        @Override
        public VectorWritable combine(VectorWritable value1, VectorWritable value2) {
            value1.set(value1.get().assign(value2.get(), Functions.PLUS));
            return value1;
        }
    }

    static public class LongWritableSum implements CombiningFunction<LongWritable> {
        @Override
        public LongWritable combine(LongWritable value1, LongWritable value2) {
            value1.set(value1.get() + value2.get());
            return value1;
        }
    }

    static public class IntWritableSum implements CombiningFunction<IntWritable> {
        @Override
        public IntWritable combine(IntWritable value1, IntWritable value2) {
            value1.set(value1.get() + value2.get());
            return value1;
        }
    }

    static public class DoubleWritableSum implements CombiningFunction<DoubleWritable> {
        @Override
        public DoubleWritable combine(DoubleWritable value1, DoubleWritable value2) {
            value1.set(value1.get() + value2.get());
            return value1;
        }
    }

    static public class FloatWritableSum implements CombiningFunction<FloatWritable> {
        @Override
        public FloatWritable combine(FloatWritable value1, FloatWritable value2) {
            value1.set(value1.get() + value2.get());
            return value1;
        }
    }
}
