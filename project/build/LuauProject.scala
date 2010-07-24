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

  val scalatest = "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.final-SNAPSHOT"
  val scalajCollection = "org.scalaj" % "scalaj-collection_2.8.0.Beta1" % "1.0.Beta2"
  val casbah = "com.novus" % "casbah_2.8.0" % "1.0.1"

  // Pig isn't presently appear to be available within Maven.
  val hadoopCore = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
  /*val pig = "org.apache.hadoop" % "pig" % "0.7.0"*/

  val scalaToolsRepo = "Scala Tools Release Repository" at "http://scala-tools.org/repo-releases"
  val scalaToolsSnapRepo = "Scala Tools Snapshot Repository" at "http://scala-tools.org/repo-snapshots"
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/"
  val apacheRepo = "Apache Release Repository" at "https://repository.apache.org/content/repositories/releases"
  val bumRepo = "Bum Networks Release Repository" at "http://repo.bumnetworks.com/releases/"
  val bumSnapsRepo = "Bum Networks Snapshots Repository" at "http://repo.bumnetworks.com/snapshots/"
}
// vim: set ts=2 sw=2 sts=2 et:
