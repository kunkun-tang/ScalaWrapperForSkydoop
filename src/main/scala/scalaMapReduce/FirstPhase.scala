package scalaMapReduce;

import com.nicta.scoobi.Scoobi._;
import Reduction._;
import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.dataStructure.Instance;
import com.nicta.scoobi.core.WireFormat;

object FirstPhase extends ScoobiApp {

	case class PointCase(val dim: Int, val coordinates: Array[Double])
	case class InstanceCase(val objID: Int, val instID: Int, val prob: Double, val dim: Int, val arr: Array[Double])

	val CC = Config.ClusterConfig;
	val srcName = CC.getString("srcName");
	val dim = CC.getInt("dim");

	def run() {

		val lines = fromTextFile(args(0))
		implicit val instFormat = WireFormat.mkCaseWireFormat(InstanceCase, InstanceCase.unapply _);

		val instMap = lines.mapFlatten(s => List(stringToCaseInstance(s)).toIterable )
												.map(i => (getPartition(i), i)).groupByKey;

		// for(inst <- instList){

		// 	.map(word => (word, 1))
		// 	.groupByKey
		// 	.combine(Sum.int)
		// }
		// counts.toTextFile(args(1)).persist
	}

	def stringToCaseInstance(line: String) = {
		val div = line.split(" ");
		var inst: InstanceCase = null;
		var arrRet = new Array[Double](dim)
		for(i<-0 until dim) arrRet(i) = div(i+2).toDouble
		if(div.length == dim + 3){
			val pt = getPoint(div);
			inst = InstanceCase(div(0).toInt, div(1).toInt, div(div.length-1).toDouble, dim, arrRet);
		}
		else println("Sth wrong in StringToInstance in Util.")
		inst
	}

	def getPoint(div: Array[String]) = {
		var arrRet = new Array[Double](dim)
		for(i<-0 until dim) arrRet(i) = div(i+2).toDouble
		val ret = PointCase(dim, arrRet);
		ret;
	}

	def getPartition(inst: InstanceCase) = {

		val nonCaseInst = caseInstanceToInstance(inst);
		val ret = Util.getPartition(nonCaseInst);
		ret; 
	}

	def arrToPoint(arr: Array[Double]) = {
		val ret = new Point(dim);
		ret.setArrValue(arr);
		ret
	}

	def caseInstanceToInstance(inst: InstanceCase) = {
		val pt = arrToPoint(inst.arr);
		val retInst = new Instance(inst.objID, inst.instID, inst.prob, inst.dim);
		retInst.pt = pt;
		retInst
	}
	
}
