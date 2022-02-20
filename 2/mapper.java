package earthquake;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,DoubleWritable> values,Reporter r) throws IOException{
        String arr[] = data.toString().split(",");
        if(arr.length!=12)
            return;
        //arr[9] for depth
        values.collect(new Text(arr[11]),new DoubleWritable(Double.parseDouble(arr[8])));
    }
}