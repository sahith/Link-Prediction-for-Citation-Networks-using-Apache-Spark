# Link-Prediction-for-Citation-Networks-using-Apache-Spark
Link Prediction is about predicting the future connections in a graph. In this project, Link Prediction is about predicting whether two authors will be collaborating for their future paper or not given the graph of authors who collaborated for atleast one paper together.


## Instructions to compile and run the code
1. Create AWS EMR cluster with spark selected in advanced options. More at [AWS EMR Instructions](https://awsemrinstructions.s3-us-west-2.amazonaws.com/GettingStartedAWS(1)(1).pdf)
2. Upload the jar file of this project in s3 bucket.
3. Add a step with the following configurations<br/> 
	**Step Type**: spark application<br/>
	**Name**: Link Prediction<br/>
	**Deploy Mode**: Cluster<br/>
	**Spark-submit options**: --class "LinkPrediction"<br/> 
	**Application Location**: s3 path of the uploaded jar file<br/>
	**Arguments**: // Specify the arguments in the given order<br/>
		s3://sxa180065/LinkPrediction1/com-dblp.ungraph.txt<br/>
		s3://{your bucket name}/CN<br/> 
		s3://{your bucket name}/JC<br/>
		s3://{your bucket name}/PA<br/>
		s3://{your bucket name}/AA<br/>
		s3://{your bucket name}/RA<br/>
		s3://{your bucket name}/ND<br/>
		s3://{your bucket name}/Logging<br/>
		s3://{your bucket name}/Predictions<br/>
	**Action on failure**: Continue 
4. Output of link prediction according to each measure can be seen in the above specified paths<br/>
	Common Neighbors count at */CN<br/> 
	Jaccard Coefficient at */JC<br/> 
	Preferential Attachment at */PA<br/>
	Adamic Score at */AA<br/>
	Resource Allocation at */RA<br/>
	Neighborhood distance at */ND<br/>
	Log Statements to */Logging<br/>
	Predictions of the supervised algorithm at */Predictions<br/>


## Dataset Link
[Citation Dataset](https://snap.stanford.edu/data/com-DBLP.html)
