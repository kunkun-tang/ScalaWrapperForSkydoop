package scalaMapReduce;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import probSkyline.dataStructure._;
import probSkyline.Util;

import scala.collection.JavaConversions._;



// This class performs the map operation, translating raw input into the key-value
// pairs we will feed into our reduce operation.
class TokenizerMapper extends Mapper[Object, Text, IntWritable, InstanceWritable] {
  var one = new IntWritable(1)
  val word = new Text
  
  override
  def map(key:Object, value:Text, context:Mapper[Object, Text, IntWritable, InstanceWritable]#Context) = {
    val inst = Util.stringToInstance(value.toString());
    val partID = Util.getPartition(inst);
		context.write(new IntWritable(partID), UtilDoop.instToinstWritable(inst));
  }
}

  
// This class performs the reduce operation, iterating over the key-value pairs
// produced by our map operation to produce a result. In this case we just
// calculate a simple total for each word seen.
class IntSumReducer extends Reducer[IntWritable,InstanceWritable,IntWritable,IntWritable] {
  override
  def reduce(key:IntWritable, values:java.lang.Iterable[InstanceWritable], context:Reducer[IntWritable,InstanceWritable,IntWritable,IntWritable]#Context) = {
    val sum = 5;
    context.write(key, new IntWritable(sum))
  }
}
  
// This class configures and runs the job with the map and reduce classes we've
// specified above.
object FirstPhase{

  def main(args:Array[String]) {
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: wordcount <in> <out>");
    }
    else{
      val job = new Job(conf, "word count")
      job.setJarByClass(classOf[TokenizerMapper])
      job.setMapperClass(classOf[TokenizerMapper])
      job.setCombinerClass(classOf[IntSumReducer])
      job.setReducerClass(classOf[IntSumReducer])
      job.setOutputKeyClass(classOf[IntWritable])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path((args(1))))
      if (job.waitForCompletion(true))
        println(" return 0");
      else
        println(" return 1");
    }
  }
}
