package probSkyline.client

import probSkyline.query._
import scalaMapReduce.Config;
import scala.collection.mutable.ListBuffer
import java.io.File
import probSkyline.query._
import probSkyline.Util

/**
 * SingleClient reads data from the folder part as the source,
 * and do pruning based on rectangle pruning, and write data to
 * .result file/
 */
object SingleClient extends App{

	val CC = Config.ClusterConfig;

	if(args.length != 2){
		println("usage: scala SingleClient naive(optimized) single(all)")
		System.exit(1);
	}
	val argStr = args(1)

	if(args(0) == "naive"){
		println("The naive computing strategy: ");
		if(argStr == "single"){
			val nqClient = new NaiveQuery(CC.getString("testArea"))
			nqClient.compProb(nqClient.getItemList)
		}
		else{
			val splitNum = CC.getInt("splitNum");
			val nqClient = new NaiveQuery(CC.getString("testArea"))
			for(i<- 0 until splitNum){
				nqClient.changeTestArea(i.toString)
				nqClient.compProb(nqClient.getItemList)
			}
		}
	}
	else{
		if(argStr == "single"){
			val tArea = CC.getString("testArea");
			val oqClient = new OptimizedQuery(tArea, OptimizedQuery.getItemMap(tArea) );
			oqClient.readMAXMIN();
			oqClient.rule1();
			oqClient.rule2();
			oqClient.rule3();
		}
		else{
			val splitNum = CC.getInt("splitNum");
			for(i<- 0 until splitNum){
				val iStr = i.toString();
				val oqClient = new OptimizedQuery(iStr, OptimizedQuery.getItemMap(iStr) );
				oqClient.readMAXMIN();
				oqClient.rule1();
				oqClient.rule2();
			}
		}
	}

	/**
	 * get the files uner a folder recursively.
	 * Since part folder also has other files, the function filters files which are ".txt" files
	 */
	def listAllFiles(fList: ListBuffer[File], path:File){
		if(path.isDirectory()){
			for{f<-path.listFiles() if f.getName().substring(f.getName().length()-3) == "txt" }{
				if(f.isFile) fList += f.getAbsoluteFile() 
				else listAllFiles(fList, f)
			}
		}
	}
}
