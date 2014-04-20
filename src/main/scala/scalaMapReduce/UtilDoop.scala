package scalaMapReduce;

import probSkyline.dataStructure._;
import probSkyline.Util;
import org.apache.hadoop.io._;

object UtilDoop{

	val dim = Util.dim;
	
	def instToinstWritable(inst: Instance) = {
		val pt = inst.pt;
		val doubleWrit = new Array[Writable](dim);
		for(i<- 0 until dim){
			doubleWrit(i) = new DoubleWritable(inst.pt(i));	
		}
		val ptWrit = new PointWritable(classOf[DoubleWritable], doubleWrit);
		val instWrit = new InstanceWritable(new IntWritable(inst.objID), new IntWritable(inst.instID), new DoubleWritable(inst.prob), ptWrit);
		instWrit;
	}
}

