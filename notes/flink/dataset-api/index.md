# Flink Dataset API

In earlier versions of Flink, the Dataset API was the primary API for stream processing. The Dataset API was 
deprecated in Flink 1.11 and replaced by the DataStream API. The Dataset API was used for batch processing and the 
DataStream API was used for stream processing, but now primarily DataStream API is used for both batch and stream processing.

## Dataset API

In our first code, I will introduce you to the Dataset API. In simple case, I will write a word count program reading a file and writing counts for each word.

Create a new Java class `WordCount` with a `main` method. The `main` method will be the application entry point.

### 1. Create Execution Environment

The first step in Flink application code is to create an execution environment. This is created using the 
`ExecutionEnvironment` class.

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
```

### 2. Read Data

The next stage is to read from any data source. This is done using the `DataSource` class. The 
`ExecutionEnvironment` you created above will be used to create the `DataSource`. This provides few methods to read 
data from files.

```java
DataSource<String> dataSource = env.readTextFile("path/to/file");
```

### 3. Transform Data

Once you have this data source, you can transform it using various operators. In this case, the data source can be 
transformed using the `flatMap` operator. This operator takes a function as an input. The function is a 
`FlatMapFunction` generic class. The first parameter is the input type and second parameter is the output type. The 
next stage is to group the data using each word. Next, you can use the grouped words to sum up the counts.

```java
FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {

        Arrays.stream(line.split("\\s+")).forEach(
                word -> out.collect(Tuple2.of(word, 1L))
        );
    }
});

UnsortedGrouping<Tuple2<String, Long>> unsortedGrouping = flatMapOperator.groupBy(0);

AggregateOperator<Tuple2<String, Long>> aggregateOperator = unsortedGrouping.sum(1);
```

The last step to execute the program is to print the results.

```java
aggregateOperator.print();
```

In our case, let's assume the input file is as below.

```plaintext
Hello Java
Hello Python
Hello Flink with Java
```

When you execute this Flink application, you will get the following output.

```plaintext
(Hello,3)
(with,1)
(Python,1)
(Java,2)
(Flink,1)
```

## Full Application Code

The full code for the word count program is as below.

```java
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * Word Count program reading a file and writing counts for each word.
 * This uses Dataset API which is deprecated in favor of DataStream API
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("path/to/file");

        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {

                Arrays.stream(line.split("\\s+")).forEach(
                        word -> out.collect(Tuple2.of(word, 1L))
                );
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> unsortedGrouping = flatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> aggregateOperator = unsortedGrouping.sum(1);

        aggregateOperator.print();
    }
}
```