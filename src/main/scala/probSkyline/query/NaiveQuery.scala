package probSkyline.query

import scalaMapReduce.Config;
import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;

class NaiveQuery(var area: String){
	
	val CC = Config.ClusterConfig;
	val folderName = CC.getString("partFolder");
	def fileName = "./" + folderName + "/" + area;
	val dim = CC.getInt("dim");


	/*
	 * read original data file's data to memory.
	 * from file to itemList, which represents the list of items.
	 */
	def getItemList = Util.getItemList(fileName);

	def changeTestArea(cArea: String){
		area = cArea;
	}

	
	/*
	 * compProb is target to use naive way to compute probability
	 * of objects.
	 */
	def compProb(itemList: List[Item]){
		println("The size of items in this partition = "+ itemList.length);
		var satisfied = 0;
		for(aItem <- itemList){
			var objSkyProb = 0.0
			for(aInst <- aItem.instances){
				var instSkyProb = 1.0;
				for(oItem <- itemList; if oItem.objID != aItem.objID){
					var itemAddition = 0.0;
					for(oInst <- oItem.instances; if oInst.checkDomination(aInst) == true)
						itemAddition += oInst.prob
					instSkyProb *= (1- itemAddition)
				}
				aInst.instSkyProb = instSkyProb
				objSkyProb += aInst.prob * aInst.instSkyProb
			}
			aItem.objSkyProb = objSkyProb
			if(objSkyProb > 0.01){ 
				satisfied += 1;
			//	println("objSkyProb = "+ objSkyProb);
			}
		}
		println(satisfied + " items remained in the partition");
	}


}
