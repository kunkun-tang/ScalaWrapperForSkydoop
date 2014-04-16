package probSkyline.dataStructure;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.HashMap;
import com.nicta.scoobi.core.WireFormat;
import java.io.DataInput;
import java.io.DataOutput;

class Instance(val objID: Int, val instID: Int, val prob: Double, val dim: Int)extends Serializable with WireFormat[Instance]{ 
	var instSkyProb = 0.0
	var pt: Point = new Point(dim)
	var instPotentialSkyline = true;

	def checkDomination(other: Instance) = pt.checkDomination(other.pt)

	def this(onePoint: Point){
		this(0, 0, 0.0, onePoint.dim)
		this.pt.setPoint (onePoint)
	}

	def setPoint(arr: Array[Double]){
		pt.setArrValue(arr)
	}

	def sum = pt.sum

	override def toString = objID.toString + " " + instID.toString + " " + pt.toString +  " " + prob.toString

	override def fromWire(in: DataInput) = {

		val ret = new Instance(1,1,1.0,1);
		ret;
	}

	override def toWire(inst: Instance, out: DataOutput) = {

		out.writeInt(inst.objID);
		out.writeInt(inst.instID);
		out.writeDouble(inst.prob);
		out.writeInt(inst.dim);
		out.writeDouble(inst.instSkyProb);
		out.writeBoolean(inst.instPotentialSkyline);
	}

}


class Item(val objID: Int) extends Serializable{
	
	var objSkyProb = 0.0
	var potentialSkyline = true;
	val instances = ListBuffer[Instance]()
	var min: Point = null
	var max: Point = null

	def addInstance(inst: Instance){
		instances += inst
	}

	def setMin(aMin: Point){
		min = aMin
	}
	
	def setMax(aMax: Point){
		max = aMax
	}

}

class PartitionInfo(val splitNo: Int) extends Serializable{


	val min = HashMap[Int, Point]();
	val max = HashMap[Int, Point]();

	def addMin(objID: Int, onePoint: Point){
		min.update(objID,onePoint);
	}

	def addMax(objID: Int, onePoint: Point){
		max.update(objID,onePoint);
	}
}
