package probSkyline.query

import scalaMapReduce.Config;
import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;
import com.typesafe.config.ConfigFactory

/*
 * Using the basic filter to prune data firstly.
 */
class MediumQuery(var file: String){

	def fileName = file;

	/*
	 * read original data file's data to memory.
	 * from file to itemList, which represents the list of items.
	 */
	def getItemList = Util.getItemList(fileName);
	
	/*
	 * compProb is target to use naive way to compute probability
	 * of objects.
	 */
	def compProb(itemList: List[Item]){
		println("The size of items in this partition = "+ itemList.length);
		var satisfied = 0;
		var count = 0
		for(aItem <- itemList){
			var objSkyProb = 0.0
			if(count%10 == 0) println("----- count: "+count + "-------");
			count += 1;
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
			if(objSkyProb > 0.005){ 
				satisfied += 1;
			//	println("objSkyProb = "+ objSkyProb);
				println(aItem.toString);
			}
		}
		println(satisfied + " items remained in the partition");
	}


	/*
	 * Read MAXMIN file from external file link.
	 */
	def readMAXMIN(){
		import java.io.FileInputStream;
		import java.io.IOException;
		import java.io.File;
		import java.io.ObjectInputStream;

		class ObjectInputStreamWithCustomClassLoader(
		  fileInputStream: FileInputStream
		) extends ObjectInputStream(fileInputStream) {
		  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		    try { Class.forName(desc.getName, false, getClass.getClassLoader) }
		    catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
		  }
		}


		try{
			val input = new FileInputStream(new File("./" + OptimizedQuery.folderName + "/MAX_MIN"));
			val in = new ObjectInputStreamWithCustomClassLoader(input);
			this.outputLists = in.readObject().asInstanceOf[ ListBuffer[PartitionInfo] ];
			in.close();
			input.close(); 
		}
		catch{
			case e: Exception => e.printStackTrace();
		}
	}

	def rule1(){
		if(outputLists != null){
			val areaInfo = outputLists(area.toInt);
			for( (idMax, ptMax) <- areaInfo.max ){
				if(itemMap(idMax).potentialSkyline == true){
					for{
						(idMin, ptMin) <- areaInfo.min
						if idMin != idMax 
						if itemMap(idMin).potentialSkyline == true
						if ptMax.checkDomination(ptMin) == true
					}itemMap(idMin).potentialSkyline = false;
				}
			}
	  }
	  removeAndGenerateNewList();
	}

	def removeAndGenerateNewList(){
//		println("before prune 1, the size of objects is " + itemMap.size);
		for( (objID, item) <- itemMap if item.potentialSkyline == true)
			cleanItemMap += objID -> item;
		println("after removing redundant items, the size decreases to " + cleanItemMap.size);
	}



}
