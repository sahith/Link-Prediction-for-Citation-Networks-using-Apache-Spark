# Link-Prediction-for-Citation-Networks-using-Apache-Spark
Link Prediction is about predicting the future connections in a graph. In this project, Link Prediction is about predicting whether two authors will be collaborating for their future paper or not given the graph of authors who collaborated for atleast one paper together.


# Instructions to compile and run the code
1. Create AWS EMR cluster with spark selected in advanced options. More at [AWS EMR Instructions](https://github.com/sahith/Link-Prediction-for-Citation-Networks-using-Apache-Spark)
2. Upload the jar file of this project in s3 bucket.
3. Add a step with the following configurations
	**Step Type**: spark application
	**Name**: Link Prediction
	**Deploy Mode**: Cluster
	**Spark-submit options**: --class "LinkPrediction"
	**Application Location**: s3 path of the uploaded jar file
	**Arguments**: // Specify the arguments in the given order
		s3://sxa180065/LinkPrediction1/com-dblp.ungraph.txt
		s3://{your bucket name}/CN
		s3://{your bucket name}/JC
		s3://{your bucket name}/PA
		s3://{your bucket name}/AA
		s3://{your bucket name}/RA
		s3://{your bucket name}/ND
		s3://{your bucket name}/Logging
		s3://{your bucket name}/Predictions
	**Action on failure**: Continue
4. Output of link prediction according to each measure can be seen in the above specified paths
	Common Neighbors count at */CN
	Jaccard Coefficient at */JC
	Preferential Attachment at */PA
	Adamic Score at */AA
	Resource Allocation at */RA
	Neighborhood distance at */ND
	Log Statements to */Logging
	Predictions of the supervised algorithm at */Predictions


## Dataset Link
[Citation Dataset](https://snap.stanford.edu/data/com-DBLP.html)
