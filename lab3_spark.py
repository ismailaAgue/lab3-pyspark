"""Lab Work 3. Introduction to Spark and PySpark DataFrames.

Dataset: Students.csv (200,000 rows)
Columns: StudentID, Name, Subject, Score, Attendance, Semester
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum, avg, count

# ---------------------------------------------------------------
# Task 1: Initialize Spark and create a session
# ---------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkBasics") \
    .getOrCreate()

print("Spark version:", spark.version)
print("Application name:", spark.sparkContext.appName)

# ---------------------------------------------------------------
# Task 2: Load dataset into a Spark DataFrame
# ---------------------------------------------------------------
df = spark.read.csv("data/students.csv", header=True, inferSchema=True)

print("\n--- Task 2: first 5 rows ---")
df.show(5)

print("--- Task 2: schema ---")
df.printSchema()
print("Total rows:", df.count())

# ---------------------------------------------------------------
# Task 3: Basic DataFrame operations
# ---------------------------------------------------------------
# 1. Select specific columns
df_selected = df.select("Subject", "Semester", "Score", "Attendance")
# 2. Filter rows based on a condition (students who scored above 50)
df_filtered = df_selected.filter(col("Score") > 50)
# 3. Add a computed column: score weighted by attendance
df_new = df_filtered.withColumn("WeightedScore",
                                expr("Score * Attendance / 100"))

print("--- Task 3: filtered rows with computed WeightedScore ---")
df_new.show(5)
print("Rows after filter (Score > 50):", df_new.count())

# ---------------------------------------------------------------
# Task 4: Group and aggregate data
# ---------------------------------------------------------------
df_agg = df_new.groupBy("Subject") \
    .agg(count("*").alias("Students"),
         sum("WeightedScore").alias("TotalWeightedScore"),
         avg("Score").alias("AverageScore"),
         avg("Attendance").alias("AverageAttendance"))

print("--- Task 4: aggregation with DataFrame API ---")
df_agg.show()

# ---------------------------------------------------------------
# Task 5: Spark SQL
# ---------------------------------------------------------------
df_new.createOrReplaceTempView("students_table")

query = """
SELECT Subject,
       COUNT(*)            AS Students,
       SUM(WeightedScore)  AS TotalWeightedScore,
       AVG(Score)          AS AverageScore,
       AVG(Attendance)     AS AverageAttendance
FROM students_table
GROUP BY Subject
ORDER BY AverageScore DESC
"""
result = spark.sql(query)

print("--- Task 5: aggregation with Spark SQL ---")
result.show()

# Extra query: average score per subject and semester
print("--- Task 5 (extra): by Subject and Semester ---")
spark.sql("""
SELECT Subject, Semester, ROUND(AVG(Score), 2) AS AverageScore
FROM students_table
GROUP BY Subject, Semester
ORDER BY Subject, Semester
""").show()

# ---------------------------------------------------------------
# Additional notes: save result and stop the session
# ---------------------------------------------------------------
result.write.mode("overwrite").csv("output/students_summary", header=True)
result.write.mode("overwrite").parquet("output/students_summary_parquet")

spark.stop()
