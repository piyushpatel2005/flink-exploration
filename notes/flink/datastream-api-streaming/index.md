# DataStream API - Stream Processing

Flink provides the DataStream API for stream processing. Previous lesson showed how you can perform word count 
operation on a batch of files. This lesson will show you how you can perform word count on a stream of data. This 
data is not bounded but Flink will keep state in memory for the duration of the stream.

## Word Count Streaming

Let's go through the creation of word count program using DataStream API. Below is a full program and I will explain 
the code further.

```java
public class WordCountStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // nc -lk 4567
        env.socketTextStream("localhost", 4567)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(line.split("\\s+"))
                                .forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                }).keyBy(pair -> pair.f0)
                .sum(1)
                .print();
        env.execute();

    }
}
```

1. Just like batch application, the first step is to create an execution environment.
2. The next step is to read the data source. In this case, I am using socket data and to read data from socket, you 
   can use `socketTextStream` method on the `StreamExecutionEnvironment`. This method needs the hostname and the 
   port on which you want to read the data.
3. The next step is to transform the data to get the word count for each word. In this example, I have chained these 
   operations into a single operation but the end result is still the same. At the end, I am calling `print()` 
   method to print the results on the console.
4. The last step is to execute the application using the `execute()` method.

## Running the Application

Now to test out this application, you can open your terminal and run the following command to open a socket on port.

```bash
nc -lk 4567
```

Once it's running, you can launch the application. The application will not terminate until you explicitly stop it. 
While the application is running, on the terminal, type the following.

```plaintext
Hello Scala
```

You will see the word count output in the application run right away. If you enter another word, you will see their 
counts being accumulated over time. This is real time word calculation on the fly.

In order to execute this application on Flink cluster, you can use the Flink command line interface. You can run
below command to start the application on Flink cluster.

```bash
./bin/flink run -d -m [JOB_MANAGER_ADDRESS] \
    -c com.github.piyushpatel2005.basics.WordCountStreaming \
    path-to-jar-file.jar
```

This command give you the Job ID of the application. This job is submitted in detached mode and the application is
running in the background. Once the application is
submitted, you can open the job manager UI and see the application running.

If you want to cancel the running application, you need job ID to cancel this application. You can execute below
command.

```bash
./bin/flink cancel [JOB_ID]
```

## Refactor the Code

You can refactor the code to use Java 8 Lambda expressions. You can modify your code to the following which is lot 
more succinct and readable.

```java
env.socketTextStream("localhost", 4567)
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.stream(line.split("\\s+"))
        .map(word -> Tuple2.of(word, 1L))
        .forEach(out::collect))
        .returns(Types.TUPLE(Types.STRING, Types.LONG))
        .keyBy(pair -> pair.f0)
        .sum(1)
        .print();
```

