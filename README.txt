Assignment 2 -

Submit by- 

	Tal Weisler 316297019 (weislert)
	Rotem Amit 314976903 (amitrote)

Details about the work-

- Number of MapReduce rounds: 5

- Number of Steps: 1

- Instace : M4Large

- Number of Instances: 2

- How to run - 
	1. update the credential - on the local dir /.aws/credentials
	2. create bucket- ass02
	3. upload Ass2.jar to ass02 bucket
	4. run the jar - java -jar DSP2.jar

- Map-Reduce jobs -  

* JOB 1 -
	
	Class: FileSpliter 

	Goal:
	- Divide the corpus
	- Calculate the N by Counting the occurrences
	- For each Trigram- Calculate the R of the first corpus and the second corpus 

	Output: 
	- File that contain : <Trigram> <R1> <R2> 
	
	Mapper:
	- Types: <LongWritable, Text, Text, CorpusOccurrences>
	- Output: 
		key - Trigram 
		Value - CorpusOccurrences which contain - corpus & occurrences

	Redducer: 
	- Types: <Text,CorpusOccurrences,Text,Text> 
	- The reducer contain String which contain the last trigram, r1 and r2. 
	  if the new key equal to the trigram, the reducer check in which corpus this line is going to be and add its occurrences to the right r.
	  if the new key diffrent from the trigram, the reduccer Emit the old trigram in both of the corpus and reset the values.    

* JOB 2 -

	Class: NrTrMaker

	Goal: 
	for each R:
	- Calculate Nr
	- Calculate Tr

	Output: 
	- File that contain : <R> <Nr> <Tr> (for each corpus)
	
	Mapper:
	- Types: <LongWritable, Text, LongWritable, Sum>
	- Output: 
		key - R
		Value - Sum which contain - corpus, R & other R

	Redducer: 
	- Types: <LongWritable, Sum,LongWritable,Sum>
	- The reducer contain long which contain the last R, and counter for Nr_corpus1, Nr_corpus2, Tr_corpus1, Tr_corpus2.
	  if the new key equal to the R, the reducer check the corpus and add 1 to the correct Nr and the otherR to the correct Tr.
	  if the new key diffrent from the R, the reduccer Emit the old <R> <Nri> <Tri> | i=1,2  in both of the corpus and reset the values.

    
* JOB 3 -

	Class: JoinCorpusData

	Goal: 
	- Join the data from job1 with the data from job2 (4 files). 
	  job1 - <Trigram> <R1> <R2>
	  job2 - <R> <Nr> <Tr>  

	Output: 
	- File that contain 2 lines to each Trigram. each line contain : <Trigram> <Nri> <Tri> | i=1,2 
	
	Mapper:
	- Types: <LongWritable, Text, Text, Text>
	- Output: 	
		If the line came from the first Job:
		key - myR_1
		Value - corpus & Trigram
		If the line came from the second Job:
		key - myR_0
		Value - corpus, Nr & Tr
	* because we add _0 and _1 to the keys we will first get the Nr,Tr of both corpus and then we will get the trigrams 

	Redducer: 
	- Types: <Text, Text,Text,Text>  
	- The reducer contain long which contain the last R, and Nr_corpus1, Nr_corpus2, Tr_corpus1, Tr_corpus2.
	  if the new key equal to the R, the reducer check the corpus and Emit <Trigram> <Nri> <Tri> | i=1,2 
	  if the new key diffrent from the R, the reduccer reset the values. as we said before, the first two line will be the Nr,Tr of both corpus

* JOB 4 -

	Class: DeletedEstimation

	Goal: 
	- calculate the DeletedEstimation of each Trigram.  
	* the Job get the result from Job3 and the N_Counter from Job1 

	Output: 
	- File that contain : <Trigram> <probability>
	
	Mapper:
	- Types: <LongWritable, Text, Text, Text>
	- Output: 	
		key - Trigram
		Value - Nr & Tr

	Redducer: 
	- Types: <Text,Text,Text, DoubleWritable>
	- The reducer contain String which contain the last Trigram, and Nr_corpus1, Nr_corpus2, Tr_corpus1, Tr_corpus2, N.
	  if the new key equal to the Trigram, the calculate the probability
	  if the new key diffrent from the R, the reduccer reset the values

* JOB 5 -

	Class: ArrangingTheResult

	Goal: 
	- Reorganize the results as required: <w1><w2><probability>

	Output: 
	- File that contain : <Trigram> <probability>
	
	Mapper:
	- Types: <LongWritable, Text, Probability,Text>
	- Output: 	
		key - <Probability> -> <w1><w2><probability>
		Value - <w3>

	Redducer: 
	- Types: <Probability,Text,Text, Text> 
	- The reducer Emit the result: <Trigram> <probability>

