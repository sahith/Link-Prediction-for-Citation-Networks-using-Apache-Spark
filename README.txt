Included scala file for project code and jar file inorder to run on AWS EMR Cluster.

Instructions to compile and run the code
1. Create AWS EMR cluster with spark selected in advanced options. More at [AWS EMR Instructions](https://github.com/sahith/Link-Prediction-for-Citation-Networks-using-Apache-Spark)
2. Upload the jar file of this project in s3 bucket.
3. Add a step with the following configurations
	step type: spark application
	Name: Link Prediction
	Deploy mode: Cluster
	Spark-submit options: --class "LinkPrediction"
	Application Location: s3 path of the uploaded jar file
	Arguments: // Specify the arguments in the given order
		s3://sxa180065/LinkPrediction1/com-dblp.ungraph.txt
		s3://{your bucket name}/CN
		s3://{your bucket name}/JC
		s3://{your bucket name}/PA
		s3://{your bucket name}/AA
		s3://{your bucket name}/RA
		s3://{your bucket name}/ND
		s3://{your bucket name}/Logging
		s3://{your bucket name}/Predictions
	Action on failure: Continue
4. Output of link prediction according to each measure can be seen in the above specified paths
	Common Neighbors count at */CN
	Jaccard Coefficient at */JC
	Preferential Attachment at */PA
	Adamic Score at */AA
	Resource Allocation at */RA
	Neighborhood distance at */ND
	Log Statements to */Logging
	Predictions of the supervised algorithm at */Predictions



[Citation Dataset Link](https://snap.stanford.edu/data/com-DBLP.html)
