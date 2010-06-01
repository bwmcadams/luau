/*
 * project/build/LuauProject.scala
 * created: 04/22/10
 * 
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * $Id: scalacommenter.vim 307 2010-04-09 01:07:43Z  $
 * 
 */

import sbt._


class LuauProject(info: ProjectInfo) extends DefaultProject(info) {
  override def compileOptions = super.compileOptions ++ Seq(Unchecked, Deprecation)
  val scalatest = "org.scalatest" % "scalatest" % "1.0.1-for-scala-2.8.0.Beta1-with-test-interfaces-0.3-SNAPSHOT"
  //val mongodb = "org.mongodb" %  "mongo-java-driver" % "1.2"
  val scalajCollection = "org.scalaj" % "scalaj-collection_2.8.0.Beta1" % "1.0.Beta2"
  // Hadoop & Pig don't presently appear to be available within Maven.
  /*val hadoopCore = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
  val pig = "org.apache.hadoop" % "pig" % "0.6.0"*/

  val scalaToolsRepo = "Scala Tools Release Repository" at "http://scala-tools.org/repo-releases"
  val scalaToolsSnapRepo = "Scala Tools Snapshot Repository" at "http://scala-tools.org/repo-snapshots"
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/"
}
// vim: set ts=2 sw=2 sts=2 et:
