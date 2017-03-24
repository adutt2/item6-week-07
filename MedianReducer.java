import java.io.IOException;
import java.util.ArrayList;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
ArrayList<Integer> temperatureList = new ArrayList<Integer>();
	double median = 0.0;
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    
		for (IntWritable value : values) {
			temperatureList.add(value.get());
			median = median(temperatureList);
			
			
		}
		context.write(key, new IntWritable((int)median));
	}
public static double median(List <Integer> list){
	double result =0.0;
	double median;
	for (int i =0; i < list.size(); i++){
		Collections.sort(list);
		int size = list.size();
		if (size%2 == 0){
			int half = size/2;
			median  = list.get(half);
		}
		else{
			int half = (size+1)/2;
			median = list.get(half-1);
		}
		result = median;
	}
	return result;
}

}
