call mvn clean package --skipTests
spark-submit --class com.test.MyApp .\target\sample-1.0-SNAPSHOT-jar-with-dependencies.jar