from pyspark.sql import SparkSession
from pyspark import SparkConf


if __name__ == '__main__':
    # CREATING SPARKSESSION
    spark = SparkSession \
            .builder \
            .appName("join-employees-and-offices-py") \
            .getOrCreate()

    # SHOW CONFIGURED
    print(SparkConf().getAll())

    # SET LOG LEVEL
    spark.sparkContext.setLogLevel("INFO")

    # PATH OF OBJECTS IN CLOUD STORAGE
    get_offices = "gs://landing_z/input/offices/offices"
    get_employees = "gs://landing_z/input/employees/employees"

    # CREATING DATAFRAME
    df_offices = spark.read \
            .format("csv") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .load(get_offices)

    # CREATING DATAFRAME
    df_employees = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(get_employees)

    print("NUMBERS PARTITIONS -> ",df_offices.rdd.getNumPartitions())
    print("NUMBERS PARTITIONS -> ",df_employees.rdd.getNumPartitions())

    print("SCHEMA DATAFRAME")
    df_offices.printSchema()

    print("SCHEMA DATAFRAME")
    df_employees.printSchema()

    print("DISPLAYING DATAFRAME")
    df_offices.show()

    print("DISPLAYING DATAFRAME")
    df_employees.show()

    print("COUNTING DATAFRAME ->",df_offices.count())
    print("COUNTING DATAFRAME -> ",df_employees.count())

    # CREATING VIEW TABLES
    df_offices.createOrReplaceTempView("offices")
    df_employees.createOrReplaceTempView("employees")

    # EXECUTINH JOIN ENTER BOTH TABLES
    result_join = spark.sql("""
    SELECT
    employees.employee_number,
    employees.last_name,
    employees.first_name,
    employees.extension,
    employees.email,
    employees.office_code AS emp_office_code,
    employees.reports_to,
    employees.job_Title,
    offices.office_code AS ofc_office_code,
    offices.city,
    offices.phone,
    offices.address_line1,
    offices.address_line2,
    offices.state,
    offices.country,
    offices.postal_code,
    offices.territory
    FROM employees INNER JOIN offices ON employees.office_code = offices.office_code;
    """)

    print("EXPLAIN")
    result_join.explain()

    print("COUNTING DATAFRAME -> ",result_join.count())
    result_join.show()

    # CREATING PARQUET FILE WITH THE RESULT
    result_join.write.format("parquet").mode("overwrite").save("gs://gold-zone/vendendores")

    #
    spark.stop()

