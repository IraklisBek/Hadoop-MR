# ATD1

This is a project on Hadoop and MapReduce implemented on University purposes. The project focuses on these issues:

  - Find Stop Words out of a document corpus and store them to a csv file
  - Check the performance of the above program by experimenting on different cases (combiner, reducers number, compression)
  - Creating Inverted Index of documents

# Guidelines to run programs

  - Exercise 1 just run it simple with Hadoop dfs jar ...
  - For Exercises 2a, 2b, 3 there are some options added. These options are:
  
```sh
-combine true   | where you set the program to run with a combiner
-numReucers 10  | where you set the program to run with 10 reducers. Of course, you can set whatever num you want
-compress true  | where you set the program to run with compression
-skip file      | where you set the program to skip patterns you dont want to include in your results (stopwords for example)
```
  - For exercise2b.InvertedIndex.java:
```sh
-doc_to_count_words doc   | where you set the program to count the number of words the doc contains
```

  # Examples of running exercise2b.InvertedIndex.java

```sh
 hadoop jar /home/cloudera/project.jar exercise2b.InvertedIndex /input /output -combiner true -skip stopwords.csv 
 hadoop jar /home/cloudera/project.jar exercise2b.InvertedIndex /input /output -skip stopwords.csv -doc_to_count_words 4 

```
  # NOTES Based on the implementation
  
  - exercise1.StopWords.java and exercise2a.StopWordsPerformance should have standard arguments /input /stopwords /topK
  - exercise2b.InvertedIndex.java should run WITHOUT a combiner and have standard arguments /input /inverted_index
  - exercise3.InvertedIndexExtention should run WITH a combiner (-combiner true) and have standard arguments /input /inverted_index_extention
  
   # Performance
   
   Performance experiments can be found on the Report.pdf
