

# MAGIC %md
# MAGIC ####1. Create a dataframe as below
# MAGIC ```
# MAGIC   +---+--------+---+------+
# MAGIC   | id|    name|age|salary|
# MAGIC   +---+--------+---+------+
# MAGIC   |100|Prashant| 45| 45000|
# MAGIC   |101|   Tarun| 36| 33000|
# MAGIC   |102|   David| 48| 28000|
# MAGIC   +---+--------+---+------+
# MAGIC ```

# COMMAND ----------

schema = "id int, name string, age short, salary double"

data_list = [(100, "Prashant", 45, 45000),
             (101, "Tarun", 36, 33000),
             (102, "David", 48, 28000)]

#sample_df = spark.createDataFrame(data=data_list)
#sample_df = spark.createDataFrame(data=data_list).toDF("id", "name", "age", "salary")
sample_df = spark.createDataFrame(data=data_list, schema=schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add following columns to your Dataframe
# MAGIC 1. increment: 10% of the salary up to 3000 maximum increment
# MAGIC 2. revised_salary: salary + increment

# COMMAND ----------

from pyspark.sql.functions import expr

salary_df = (
    sample_df.withColumns({
        "increment": expr("case when salary > 30000 then 3000 else salary * 10/100 end"),
        "revised_salary": expr("salary + increment")
    })
)

salary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Add following columns to your Dataframe
# MAGIC * increment: 10% of the salary up to 3000 maximum increment
# MAGIC
# MAGIC Replace the following column in your dataframe
# MAGIC * salary: current salary + increment

# COMMAND ----------

from pyspark.sql.functions import expr

salary_df = (
    sample_df.withColumn("increment", expr("case when salary > 30000 then 3000 else salary * 10/100 end"))
        .withColumn("salary", expr("salary + increment"))
)

salary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Add a batch number (uuid) column to your dataframe

# COMMAND ----------

import uuid
from pyspark.sql.functions import lit

batch_id = str(uuid.uuid4())

salary_batch_df = sample_df.withColumn("batch_id", lit(batch_id))

salary_batch_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Rename the dataframe colums as listed below
# MAGIC 1. increment - annual_increment
# MAGIC 2. salary - incremented_salary

# COMMAND ----------

new_salary_df = (
    salary_df.withColumnsRenamed({
        "increment": "annual_increment",
        "salary": "incremented_salary"
    })
)

new_salary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Remove the following colums from your dataframe
# MAGIC 1. age
# MAGIC 2. annual_increment

# COMMAND ----------

small_salary_df = new_salary_df.drop("age", "annual_increment")

small_salary_df.display()
