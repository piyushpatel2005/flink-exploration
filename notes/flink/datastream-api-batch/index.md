# DataStream API - Batch Processing

The Datastream API gets its name from the special `DataStream` class which is used to represent a collection of data 
in a Flink application. These are immutable collection of data the can contain duplicate. The data can be bounded or 
unbounded, and the data can be in order or out of order. Apache Flink unified the APIs for batch and stream processing.

Let's go through creation of word count program using DataStream API.

## DataStream API

The DataStream API is used for stream processing. The DataStream API is used for both batch and stream processing.

### 1. Create Execution Environment

The first step in Flink DataStream API code is to create an execution environment. This is created using the 
`StreamExecutionEnvironment` class by calling the `getExecutionEnvironment` method.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

### 2. Read Data

Again, the next step is to connect to a data source and read the data. The data source is created using the 
`DataStreamSource` class. The `StreamExecutionEnvironment` you created above will be used to create the 
`DataStreamSource`.
In 
this example, I want to read the file content from `input/words.txt`. I will use the `readTextFile` method to read 
the file. This method is deprecated in favor of `readFile` method.

```java
DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");
```

### 3. Transform Data

The next step is to transform the data the way we want in our output. In this case, we have `DataStreamSource` which 
are lines of text from the input file. We want to flatten the lines of text into words. We will use the `flatMap` 
operator for this. The `flatMap` operator takes a function as an input. The function is a `FlatMapFunction` generic 
class. The first parameter is the input type and second parameter is the output type. In our case, the input type is 
`String` and the output type is `Tuple2<String, Long>` which are like a key-value pair.

```java
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
        Arrays.stream(line.split("\\s+"))
                .forEach(word -> out.collect(Tuple2.of(word, 1L)));
    }
});
```

The next step is to group the 
data using each word which is the first item in the key-value pair. Once we have grouped the data for each word, 
we need to sum up their counts.

```java
KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndCount.keyBy(pair -> pair.f0);
SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
```

Finally we apply all these computations using `print()` method. In DataStream API, we always need to execute the 
application using the `execute()` method at the end.

```java
sum.print();
env.execute();
```

## Running your application

In this demo run, my input file contains following content.

```plaintext
Hello Java
Hello Python
Hello Flink with Java
```

Running this application produces results which are similar to the output of the batch program.

```plaintext
1> (Hello,1)
1> (Hello,2)
1> (Hello,3)
4> (Java,1)
4> (Python,1)
4> (Flink,1)
4> (with,1)
4> (Java,2)
```

In this case, the application outputs intermediate results along with final result. That's why you see multiple 
entries for the same word. 

### Setting Runtime Mode

By default, the application executes as a streaming application. If you want to set the 
application mode to batch, you can use the `setRuntimeMode` method.

In order to set the batch mode, write this line after the creation of the `StreamExecutionEnvironment` object.

```java
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

In production applications, you could also set this mode using the `FLINK_RUNTIME_MODE` environment variable or by 
passing it as a command line argument.


```bash
./bin/flink run -Dexecution.runtime-mode=BATCH
```

## Full Code

The full code for this application is below.

```java
public class WordCountDataStreamBatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                Arrays.stream(line.split("\\s+"))
                        .forEach(word -> out.collect(Tuple2.of(word, 1L)));
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndCount.keyBy(pair -> pair.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();
        env.execute();
    }
}
```

## Refactor Application

Java 8 introduces `flatMap` method which is the same as the `flatMap` operator in DataStream API.

```java
var wordAndCount = dataStreamSource
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.stream(line.split("\\s+"))
        .forEach(word -> out.collect(Tuple2.of(word, 1L))));
```

Now, this code compiles fine. However, when you try running this application, you might run into following error.

```plaintext
The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
```

The solution to this problem is to explicitly specify what type of the `Collector` is. In our case, the type of the 
`Collector` is `Collector<Tuple2<String, Long>>`. You can do this using the `returns` method.

```java
var wordAndCount = dataStreamSource
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.stream(line.split("\\s+"))
        .forEach(word -> out.collect(Tuple2.of(word, 1L))))
        .returns(Types.TUPLE(Types.STRING, Types.LONG));
```

Now, if you run the application, it works fine.