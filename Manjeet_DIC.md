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


# 2. MapReduce for Sum of Cubes
## Mapper:

``` java
public class CubeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int num = Integer.parseInt(value.toString());
        long cube = (long) num * num * num;
        String type = (num % 2 == 0) ? "even" : "odd";
        if (isPrime(num)) {
            type = "prime";
        }
        context.write(new Text(type), new LongWritable(cube));
    }

    private boolean isPrime(int n) {
        if (n <= 1) return false;
        for (int i = 2; i <= Math.sqrt(n); i++) {
            if (n % i == 0) return false;
        }
        return true;
    }
}


