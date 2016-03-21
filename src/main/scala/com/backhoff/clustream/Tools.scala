package com.backhoff.clustream

import java.io._
import java.nio.file.{Paths, Files}

object Tools {

    def convertMCsBinariesToText(dirIn: String = "", dirOut: String = "", limit: Int): Unit = {
      print("processing files: ")
      for(i <- 0 to limit) {
        if(Files.exists(Paths.get(dirIn + "/" + i)))
        try {
          val file = new ObjectInputStream(new FileInputStream(dirIn + "/" + i))
          val mc = file.readObject().asInstanceOf[Array[MicroCluster]]
          var text: Array[String] = null
          file.close()
          if(mc != null) {
            text = mc.map { m =>
              "=========================================================== \n" +
              "MicroCluster IDs = " + m.getIds.mkString("[", ",", "]") + "\n" +
                "CF2X = " + m.getCf2x.toArray.mkString("[", ",", "]") + "\n" +
                "CF1X = " + m.getCf1x.toArray.mkString("[", ",", "]") + "\n" +
                "CF2T = " + m.getCf2t.toString + "\n" +
                "CF1T = " + m.getCf1t.toString + "\n" +
                "N = " + m.getN.toString + "\n"
            }

            val pw = new PrintWriter(new File(dirOut + "/" + i))
            pw.write(text.mkString("","",""))
            pw.close
            print(i + " ")
          }

        }
        catch {
          case ex: IOException => println("Exception while reading files " + ex)
            null
        }
      }
      println()
    }

}

