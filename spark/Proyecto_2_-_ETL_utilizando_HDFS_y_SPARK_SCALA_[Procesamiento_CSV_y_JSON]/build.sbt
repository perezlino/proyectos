// Archivo: build.sbt

name := "etl-spark-scala"

version := "1.0"

scalaVersion := "2.12.4"

// Dependencias de Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

// Configuración para el ensamblado (assembly)
// Define el nombre del archivo JAR resultante
assemblyJarName := "etl-spark-scala-assembly.jar"

// Estrategia de merge para el ensamblado (assembly)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

