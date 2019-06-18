resolvers += Resolver.jcenterRepo

addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.5")

addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.20.0")

libraryDependencies += "io.packagecloud.maven.wagon" % "maven-packagecloud-wagon" % "0.0.6"

