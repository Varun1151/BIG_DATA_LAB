package earthquake;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    public void reduce(Text region,Iterator<DoubleWritable> magns,OutputCollector<Text,DoubleWritable> values,Reporter r) throws IOException{
        double max = Double.MIN_VALUE;
        while(magns.hasNext()){
            max = Math.max(max,magns.next().get());
        }
        values.collect(region,new DoubleWritable(max));
    }
}