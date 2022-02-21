package matmul;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,Text> values,Reporter r) throws IOException{
        int m=2,p=3;
        String arr[] = data.toString().split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if(arr[0].equals("A")){
            for(int k=0;k<p;k++){
                outputKey.set(arr[1]+","+k);
                outputValue.set("A,"+arr[2]+","+arr[3]);
                values.collect(outputKey,outputValue);
            }
        }
        else{
            for(int k=0;k<m;k++){
                outputKey.set(k+","+arr[2]);
                outputValue.set("B,"+arr[1]+","+arr[3]);
                values.collect(outputKey,outputValue);
            }
        }
    }
}