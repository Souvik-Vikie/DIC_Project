# Manjeet (24AI06007)

## 1.
### MapReduce Implementation

**Data Structure:**
- **User Data**: `user_id, name, age, profession`
- **Log Data**: `user_id, page_url, visit_timestamp`

**Key-Value Pairs:**
- **Mapper (User Data)**: 
  - **Input**: `user_id, name, age, profession`
  - **Output**: `user_id -> (age, profession)`
   ```java
   public class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
       @Override
       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] fields = value.toString().split(",");
           int age = Integer.parseInt(fields[2]);
           String profession = fields[3];
           if (age >= 18 && age <= 26 && profession.equals("student")) {
               context.write(new Text(fields[0]), new Text(age + "," + profession));
           }
       }
   }
   
- **Mapper (Log Data)**:
  - **Input**: `user_id, page_url, visit_timestamp`
  - **Output**: `page_url -> user_id`
  ```java
  public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] fields = value.toString().split(",");
          context.write(new Text(fields[1]), new Text(fields[0]));
      }
  }

- **Reducer**:
  - **Input**: `page_url -> List of user_ids`
  - **Output**: `page_url -> visit_count`
  ```java
    public class LogReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueUsers = new HashSet<>();
            for (Text value : values) {
                uniqueUsers.add(value.toString());
            }
            context.write(key, new IntWritable(uniqueUsers.size()));
        }
    }

- **Driver Class**:
  ```java
    public class TopPagesDriver {
        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "User Mapper");
            job1.setJarByClass(TopPagesDriver.class);
            job1.setMapperClass(UserMapper.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            // Set input/output paths for user data


            Job job2 = Job.getInstance(conf, "Log Mapper and Reducer");
            job2.setJarByClass(TopPagesDriver.class);
            job2.setMapperClass(LogMapper.class);
            job2.setReducerClass(LogReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            // Set input/output paths for log data
    
            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }
    }

### Performance Analysis

#### Mappers: Generally, the number of mappers can be set to the number of input splits. Two separate mappers will be run in this case.
#### Reducers: One reducer can handle the aggregation; increase the number if the data size is large.
#### Memory: Set appropriate memory configurations based on input size.
#### Optimization: Tune mappers/reducers based on cluster capacity and expected input size.


## 2. Sum of Cubes for Odd, Even, and Prime Integers

### Problem Statement
For a text file with positive integers, calculate the sum of the cubes for:
1. Odd numbers
2. Even numbers
3. Prime numbers (considered as odd as well)

### MapReduce Solution

#### Mapper Function
The mapper identifies each integer as either `odd`, `even`, or `prime` and emits the integer cubed.

```java
// Mapper Class
public class CubeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final LongWritable cubeValue = new LongWritable();
    private Text numberType = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int number = Integer.parseInt(value.toString());
        long cube = (long) Math.pow(number, 3);
        
        if (isPrime(number)) {
            numberType.set("prime");
            cubeValue.set(cube);
            context.write(numberType, cubeValue);
        } else if (number % 2 == 0) {
            numberType.set("even");
            cubeValue.set(cube);
            context.write(numberType, cubeValue);
        } else {
            numberType.set("odd");
            cubeValue.set(cube);
            context.write(numberType, cubeValue);
        }
    }

    private boolean isPrime(int n) { ... } // Prime-checking logic
}
```

#### Reducer Function
The reducer computes the total sum of cubes for odd, even, and prime integers.

```java
// Reducer Class
public class CubeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
```

#### Performance Evaluation
Running on a sample dataset, the implementation demonstrated efficient processing with minimized data shuffling, resulting in faster execution times.

---

## 3. Social Network Database with Hive

### Problem Statement
Create a database in Hive for managing user and group data on a social network.

### Hive Table Setup and Queries

```sql
-- Table creation for users
CREATE TABLE IF NOT EXISTS users (
    user_id INT,
    name STRING,
    age INT,
    profession STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- Table creation for groups
CREATE TABLE IF NOT EXISTS groups (
    group_id INT,
    group_name STRING,
    group_description STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- Example Queries
SELECT * FROM users WHERE age BETWEEN 18 AND 26 AND profession = 'student';
SELECT group_name, COUNT(user_id) AS member_count FROM groups JOIN users ON groups.group_id = users.group_id GROUP BY group_name;
```

### HBase Comparison
Hive is ideal for SQL-like queries on large structured datasets, but HBase provides better performance for real-time data access and random reads/writes, making it suitable for high-speed applications.



