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
  - For Exercise 2b:
```sh
-doc_to_count_words doc   | where you set the program to count the number of words the doc contains
```

#Examples of running exercise 2b

```sh
 hadoop jar /home/cloudera/project.jar exercise2b.InvertedIndex /shakespeare /inverted_index -combiner true -skip stopwords.csv 
 hadoop jar /home/cloudera/project.jar exercise2b.InvertedIndex /shakespeare /inverted_index -skip stopwords.csv -doc_to_count_words 4 

```
