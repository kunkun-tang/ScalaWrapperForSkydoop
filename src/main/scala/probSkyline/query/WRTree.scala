package probSkyline.query

import scalaMapReduce.Config;
import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;
import scala.collection.mutable.HashMap;
import scala.math;
import com.typesafe.config.ConfigFactory

class WRTree(val cleanItemMap: HashMap[Integer, Item], val testArea: String){

	val pivotPoints: ListBuffer[Point] = genPivotPoints();
	def area = testArea.toInt;
	val separMap = new HashMap[Int, Separation]();

	def init(){
		for(i<-0 to WRTree.numPivot) separMap += i-> new Separation();
	}

	def genPivotPoints() = {
		var retPivotPoints = new ListBuffer[Point]();
		val retList = Util.computeArrDouble();

		if(WRTree.dim == 2){
			val arrDouble = retList(0);
			var bottem = 0.toDouble;
			if(area > 0) bottem = arrDouble(area-1);
			val top = arrDouble(area);

			val lineAngle = (bottem + top)/2;

			if(lineAngle < math.Pi/4){
				for( i<- 1 to WRTree.numPivot ){
					val x = (1.toDouble)/ (WRTree.numPivot.toDouble) * 1 * i;
					val y = math.tan(lineAngle) * x;
					val aPoint = new Point(2);
					aPoint.coordinates(0) = x;
					aPoint.coordinates(1) = y;
					retPivotPoints += aPoint;
				}
			}
			else{
				for( i<- 1 to WRTree.numPivot ){
					val y = (1.toDouble)/ (WRTree.numPivot.toDouble) * 1 * i;
					val x = y / math.tan(lineAngle);
					val aPoint = new Point(2);
					aPoint.coordinates(0) = x;
					aPoint.coordinates(1) = y;
					retPivotPoints += aPoint;
				}
			}
		}
		else if(WRTree.dim == 3){

			/*
			 * TODO: given arrDouble list, we should find the 
			 * angle between the two lines.
			 */
		}
//		println("retPivotPoints = "+ retPivotPoints);
		retPivotPoints;
	}

	def collectInfo(){
		for((objID, item)<- cleanItemMap; instance<- item.instances){
			
			/*
			 * It first located the separation area where the instance should be.
			 * Then, it updates the aggregate info in that separation.
			 */
			val separID = findRightSeparation(instance);
			separMap(separID).update(instance, item);
		}
	}

	def findRightSeparation(inst: Instance) = {
		/*
		 * 0 represents the separation area which no instances are located.
		 * 1- numPivot denotes the regular separation.
		 */
		var retSepar = WRTree.numPivot;
		for( i<- pivotPoints.length-1 to (0, -1) )
			if( inst.pt.checkDomination(pivotPoints(i))) retSepar = i;
		retSepar;
	}

	def findLeftSeparation(inst: Instance) = {
		/*
		 * 0 represents the separation area which no instances are located.
		 * 1- numPivot denotes the regular separation.
		 */
		var retSepar = 0;
		for( i<- 0 until pivotPoints.length)
			if( pivotPoints(i).checkDomination(inst.pt)) retSepar = i;
		retSepar;
	}


	def iterCompProb(){
		for( (objID, item) <- cleanItemMap; instance <- item.instances; 
			if instance.instPotentialSkyline == true){

			/*
			 * lSepar represents the separation that lSepar-1 fully
			 * dominates the instance, and the instance fully dominates
			 * rSepar.
			 */
			val rSepar = findRightSeparation(instance);
			val lSepar = findLeftSeparation(instance);

			val globalAggregateMap = new HashMap[Int, Double]();
			if(lSepar > 0){
				for(i<-0 until lSepar){
					separMap(i).updateGlobalAggregateMap(globalAggregateMap);
				}
			}

			for(i<- lSepar to rSepar)
				separMap(i).updateGlobalAggregateMap(globalAggregateMap);

			instance.instSkyProb = multiplyMap(instance, globalAggregateMap)
		}
	}

	def multiplyMap(inst: Instance, aggregateMap: HashMap[Int, Double])={
		var ret = 1.0;
		for((objID, prob)<- aggregateMap; if inst.objID != objID)
			ret *= prob;
		ret;
	}

	def run(){
		init();
		collectInfo();
		iterCompProb();
	}
}


object WRTree{

	val conf = ConfigFactory.load;
	def folderName = conf.getString("Query.partFolder");
	def dim = conf.getInt("Query.dim");
	def numPivot = conf.getInt("Query.numPivot");
}

class Separation{

	// itemMap is the subset of cleanItemMap in OptimizedQuery.
	val itemMap = new HashMap[Int, ListBuffer[Instance]]();
	
	// aggregateMap is the sum of instance existing prob
	// of cleanItemMap in OptimizedQuery.
	val aggregateMap = new HashMap[Int, Double]();

	def update(inst: Instance, item: Item){
		if( !itemMap.contains(item.objID)){
			val instList = new ListBuffer[Instance]();
			itemMap += item.objID -> instList;
			aggregateMap += item.objID -> 0.0;
		}
		itemMap(item.objID) += inst;
		aggregateMap(item.objID) += inst.prob;
	}

	def updateGlobalAggregateMap(agMap: HashMap[Int, Double]){
		for( (objID, agProb)<- aggregateMap ){
			if(!agMap.contains(objID))
				agMap += objID -> 0.0;
			agMap(objID) += agProb;
		}
	}

}