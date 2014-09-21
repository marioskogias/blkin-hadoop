package hadoopCode;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class RecentReduce extends
		Reducer<Text, LongWritable, LongWritable, NullWritable> {

	private LongWritable diff = new LongWritable();
	private NullWritable myNull = NullWritable.get();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<LongWritable> it = values.iterator();
		LongWritable v1 = new LongWritable();
		LongWritable v2 = new LongWritable();
		try {
			v1.set(it.next().get());
			v2.set(it.next().get());
		} catch (NoSuchElementException e) {
			return;
		}
		System.out.format("v1 = %d v2= %d\n", v1.get(), v2.get());
		
		if (v1.get() > v2.get())
			diff.set(v1.get() - v2.get());
		else 
			diff.set(v2.get() - v1.get());
		
		context.write(diff, myNull);
	}
}