import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}

object LinkPrediction {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("LinkPrediction")
    .getOrCreate()

  import spark.implicits._
  // Unsupervised methods for link prediction
  def linkPredictionMeasures(dataset: DataFrame, adjacency_list: DataFrame, common_neighbors_path: String, jaccard_coefficient_path: String, pref_attach_path: String, adamic_score_path: String, resource_allocation_path: String, neighborhood_distance_measure_path: String): DataFrame = {
    // adding source node neighbors to the dataset
    val df1 = dataset.join(adjacency_list, dataset.col("source_node") === adjacency_list.col("source")).select("source_node", "destination_node", "neighbors", "label").toDF("source_node", "destination_node", "src_neighbors", "label")

    // adding destination node neighbors to the dataset
    val df2 = df1.join(adjacency_list, df1.col("destination_node") === adjacency_list.col("source")).select("source_node", "destination_node", "src_neighbors", "neighbors", "label").toDF("source_node", "destination_node", "src_neighbors", "dest_neighbors", "label")


    // Common neighbors count measure
    val common_neighbors_count = udf {
      (Set1: WrappedArray[Int], Set2: WrappedArray[Int]) =>
        (Set1.toList.intersect(Set2.toList)).size}

    val common_neighbors_df = df2.withColumn("common_neighbors_count", common_neighbors_count($"src_neighbors", $"dest_neighbors"))

    val common_neighbors_df_output = common_neighbors_df.select("source_node", "destination_node", "common_neighbors_count").sort($"common_neighbors_count".desc).limit(100)
    common_neighbors_df_output.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(common_neighbors_path)

    // Jaccard coefficient measure
    val jaccard_coefficient = udf {
      (Set1: WrappedArray[Int], Set2: WrappedArray[Int]) =>
        (Set1.toList.intersect(Set2.toList)).size.toDouble/(Set1.toList.union(Set2.toList)).distinct.size.toDouble}

    val jaccard_coefficient_df = common_neighbors_df.withColumn("jaccard_coefficient", jaccard_coefficient($"src_neighbors", $"dest_neighbors"))

    val jaccard_coefficient_df_output = jaccard_coefficient_df.select("source_node", "destination_node", "jaccard_coefficient").sort($"jaccard_coefficient".desc).limit(100)
    jaccard_coefficient_df_output.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(jaccard_coefficient_path)

    // preferential attachment score
    val preferential_attachment_score = udf {
      (Set1: WrappedArray[Int], Set2: WrappedArray[Int]) =>
        (Set1.toList.size*Set2.toList.size) }

    val pref_attach_score_df = jaccard_coefficient_df.withColumn("pref_attach_score", preferential_attachment_score($"src_neighbors", $"dest_neighbors"))

    val pref_attach_score_df_output = pref_attach_score_df.select("source_node", "destination_node", "pref_attach_score").sort($"pref_attach_score".desc).limit(100)
    pref_attach_score_df_output.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(pref_attach_path)

    // Adamic score and Resource Allocation Index
    val common_neighbors = udf {
      (Set1: WrappedArray[String], Set2: WrappedArray[String]) =>
        (Set1.toList.intersect(Set2.toList)) }

    // Appending common neighbors column to the dataframe
    val df3 = pref_attach_score_df.withColumn("common_neighbors", common_neighbors($"src_neighbors", $"dest_neighbors")).select("source_node", "destination_node", "common_neighbors")

    val common_neighbors_explode_df = df3.withColumn("common_neighbors_explode", explode($"common_neighbors")).select("source_node", "destination_node", "common_neighbors_explode")

    val neighbors_count_df = adjacency_list.withColumn("degree", size($"neighbors"))

    val df4 = common_neighbors_explode_df.join(neighbors_count_df, common_neighbors_explode_df.col("common_neighbors_explode") === neighbors_count_df.col("source")).select("source_node", "destination_node", "common_neighbors_explode", "degree")

    // expression for adamic score
    val exp1 = "sum(1.0/log10(degree))"
    // expression for resource allocation index
    val exp2 = "sum(1.0/degree)"

    val adamic_and_resource_allocation_df = df4.groupBy($"source_node", $"destination_node").agg(expr(exp1).as("adamic_score"), expr(exp2).as("resource_allocation_index")).select("source_node", "destination_node", "adamic_score", "resource_allocation_index").toDF("source", "destination", "adamic_score", "resource_allocation_index")

    val adamic_output_df = adamic_and_resource_allocation_df.select("source", "destination", "adamic_score").sort($"adamic_score".desc).limit(100)
    adamic_output_df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(adamic_score_path)

    val resource_allocation_output_df = adamic_and_resource_allocation_df.select("source", "destination", "resource_allocation_index").sort($"resource_allocation_index".desc).limit(100)
    resource_allocation_output_df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(resource_allocation_path)

    // Neighborhood distance measure
    val neighborhood_distance_score = udf {
      (common_neighbor_count: Double, pref_attach_score: Double) =>
        common_neighbor_count/math.sqrt(pref_attach_score) }

    val neighborhood_distance_score_df = pref_attach_score_df.withColumn("neighborhood_distance_score", neighborhood_distance_score($"common_neighbors_count", $"pref_attach_score"))

    val neighborhood_distance_output_df = neighborhood_distance_score_df.select("source_node", "destination_node", "neighborhood_distance_score").sort($"neighborhood_distance_score".desc).limit(100)
    neighborhood_distance_output_df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(neighborhood_distance_measure_path)

    // Source node degree measure
    val source_node_degree_df = neighborhood_distance_score_df.withColumn("source_node_degree", size($"src_neighbors"))

    val link_prediction_measures = source_node_degree_df.join(adamic_and_resource_allocation_df, source_node_degree_df("source_node") === adamic_and_resource_allocation_df("source") && source_node_degree_df("destination_node") === adamic_and_resource_allocation_df("destination")).select("source_node", "destination_node", "common_neighbors_count", "jaccard_coefficient", "pref_attach_score", "adamic_score", "resource_allocation_index", "neighborhood_distance_score", "source_node_degree", "label")
    link_prediction_measures
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      println("Incorrect number of arguments")
    }

    var result = ""

    // Reading the dataset of the DBLP graph
    val graphSchema = new StructType()
      .add("source_node",IntegerType,true)
      .add("destination_node",IntegerType,true)
    val dblp_graph = spark.read.option("header", "true").option("sep", "\t").option("inferSchema","true").schema(graphSchema).csv(args(0))
    val reversed_dblp_graph = dblp_graph.select("destination_node", "source_node").toDF("source_node", "destination_node")
    val dataset = dblp_graph.union(reversed_dblp_graph)

    // Total edges
    val edges_count = dataset.count()
    result = result + "Total number of edges in the DBLP dataset " + edges_count + "\n\n";

    // Total nodes
    result = result + "Total number of people/nodes are " + dataset.select("source_node").distinct.count() + "\n\n";

    val adj_list = dataset.groupBy("source_node").agg(collect_list("destination_node") as "neighbors").toDF("source", "neighbors")

    // Getting all 2 length missing edges
    val dataset1 = dataset.toDF("source", "destination")
    val graph_missing_edges = dataset.join(dataset1, dataset.col("destination_node") === dataset1.col("source")).select("source_node", "destination")

    // filter self edges
    val df11 = graph_missing_edges.filter($"source_node" =!= $"destination").toDF("source_node", "destination_node")
    // filter existing edges
    val df12 = df11.except(dataset)

    // Train and Test Split
    val train_split: Double = 0.8
    val test_split: Double = 0.2

    val dataset_with_label = df11.intersect(dblp_graph).withColumn("label", lit(1))
    //       println("dataset with label count "+dataset_with_label.count())

    val limit_missing_edges_count = dataset_with_label.count()

    // Limit the missing edges to the count of the existing edges
    val missing_edges = df12.limit(limit_missing_edges_count.toInt)

    val missing_edges_with_label = missing_edges.withColumn("label", lit(0))

    val links_with_label = dataset_with_label.union(missing_edges_with_label)

    result = result + "Total number of links(Hidden and Missing) are " + links_with_label.count() + "\n\n";


    val links_with_measures_df = linkPredictionMeasures(links_with_label, adj_list, args(1), args(2), args(3), args(4), args(5), args(6))

    result = result + "train_split is "+ train_split*100 + " and test_split is "+ test_split*100 + "\n\n";

    // Training the above dataset with random forest classifier
    val hasher = new FeatureHasher().setInputCols(Array("resource_allocation_index","common_neighbors_count","adamic_score","neighborhood_distance_score","pref_attach_score","jaccard_coefficient","source_node_degree")).setOutputCol("features")
    val hashed_df = hasher.transform(links_with_measures_df)

    // Normalize each Vector using $L^2$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
    val l2NormData = normalizer.transform(hashed_df)

    val Array(trainingData, testData) = l2NormData.randomSplit(Array(train_split, test_split))

    trainingData.cache()
    testData.cache()

    result = result + "Training dataset length "+ trainingData.count()+ "\n\n";

    result = result + "Testing dataset length "+ testData.count()+ "\n\n";

    val sc = spark.sparkContext
    val result_Rdd: RDD[String] = sc.parallelize(List(result))
    result_Rdd.coalesce(1, true).saveAsTextFile(args(7))

//    // Train a GBT model.
//    val gbt = new GBTClassifier()
//      .setLabelCol("label")
//      .setFeaturesCol("normFeatures")
//      .setMaxIter(5)
//      .setFeatureSubsetStrategy("auto")
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("normFeatures")
      .setNumTrees(1)

    // Train model. This also runs the indexers.
    val model = rf.fit(trainingData)

    // Make predictions
    val predictions = model.transform(testData)

    // Select 10,000 example rows to display.
    predictions.select("prediction", "label").limit(10000)
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(args(8))

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println(s"Accuracy = ${(accuracy)}")
    println(s"Test Error = ${(1.0 - accuracy)}")
  }
}