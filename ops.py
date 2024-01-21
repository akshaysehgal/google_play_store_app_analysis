from pyspark.ml.feature import Bucketizer
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when, count
from itertools import combinations
from pyspark.sql.types import StructType, StructField, StringType
import math
from pyspark.sql import DataFrame, Column
from functools import reduce
from pyspark.sql.functions import lit
from pyspark.sql.functions import concat_ws, lit
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr
from pyspark.sql.functions import lit, when, coalesce


numerical_ranges = {}

class DataTransformation:


    # Create an empty DataFrame with the empty schema

    def __init__(self, app_name="CSV Reader"):
        empty_schema = StructType([])
        self.app_name = app_name
        self.spark = SparkSession.builder.appName(self.app_name).config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
        self.df = self.spark.createDataFrame([], schema=empty_schema)

    def read_csv(self, csv_file_path, header=True, infer_schema=True):
        print("Reading CSV file...")
        self.df = self.spark.read.csv(csv_file_path, header=header, inferSchema=infer_schema)
        print("Showing the first 2 rows of the DataFrame:")
        self.df.show(2)

        selected_columns = ["genre", "minInstalls", "ratings"]
        numerical_columns = ["minInstalls", "ratings"]

        all_combinations = []
        for r in range(1, len(selected_columns) + 1):
            all_combinations.extend(combinations(selected_columns, r))

        for i in range(len(numerical_columns)):
            print(f"Binning numerical field: {numerical_columns[i]}")
            self.bin_numerical_field(numerical_columns[i])
        print("DONE BINNING")
        insights_list = [self.generate_insights(combination) for combination in all_combinations]

        for idx, insight_df in enumerate(insights_list):
            print(f"Schema of DataFrame {idx + 1}:")
            insight_df.show()

        print("Combining the results into a single DataFrame using reduce...")
        final_insights_df = reduce(lambda df1, df2: df1.union(df2), insights_list)

        print("Writing the insights to a CSV file...")
        final_insights_df.write.csv("output_insights.csv", header=True, mode="overwrite")

    def generate_insights(self, combination):
        print("Generating insights for combination:")
        print(combination)

        grouped_df = self.df.groupBy(*combination[:-1]).agg(count("*").alias("Count"))
        total_volume = self.df.count()
        insights = grouped_df.filter((grouped_df["Count"] / total_volume) >= 0.02)

        insights = insights.withColumn(
            "Conditions",
            concat_ws("; ", *[lit(f"{col_name}={col_val}") for col_name, col_val in zip(combination, insights.columns[:-2])])
        )

        return insights

    def bin_numerical_field(self, column_name):
        print(f"Binning numerical field: {column_name}")
        unique_values_count = self.df.select(column_name).distinct().count()
        if unique_values_count < 20:
            return col(column_name)
        self.df = self.df.withColumn(column_name, col(column_name).cast("double"))
        num_bins = self.calculate_bins_count(unique_values_count)
        discretizer = QuantileDiscretizer(numBuckets=num_bins, inputCol=column_name, outputCol=column_name + "_bucket")
        model = discretizer.setHandleInvalid("keep").fit(self.df)
        bin_df = model.transform(self.df)
        bucket_boundaries = model.getSplits()
        bin_ranges = [(start, end) for start, end in zip(bucket_boundaries[:-1], bucket_boundaries[1:])]
        print("BIN RANGES")
        print(bin_ranges)
        bin_ranges_str = [f"[{start}, {end})" for start, end in bin_ranges]

        bin_df = bin_df.withColumn(column_name + "_bin_range", lit(bin_ranges_str))
        bin_df = bin_df.withColumn(column_name + "_specific_bucket", lit('Other'))

        for (start, end), range_str in zip(bin_ranges, bin_ranges_str):
            bin_df = bin_df.withColumn(
                column_name + "_specific_bucket",
                when((bin_df[column_name] >= start) & (bin_df[column_name] < end), range_str)
                .otherwise(bin_df[column_name + "_specific_bucket"])
            )

        self.df = self.df.join(bin_df.select(column_name, column_name + "_specific_bucket"), on=column_name)

        bin_df.select(column_name + "_specific_bucket").show()

        # return bin_df, bin_ranges
        # return bin_df[column_name+"_specific_bucket"]


        # df1 = model.transform(df)
        #
        # # Extract the bucket boundaries
        # bucket_boundaries = model.getSplits()
        # bucket_ranges_list = [(start, end) for start, end in zip(bucket_boundaries[:-1], bucket_boundaries[1:])]
        #
        # # Display the bucket ranges list
        # print(f"Bucket Ranges List for {column_name}: {bucket_ranges_list}")
        #
        # # Add bucket_ranges_list to the dictionary
        # numerical_ranges[column_name + "_bucket"] = bucket_ranges_list
        #
        # print(numerical_ranges)














        # # Convert the column to IntegerType
        # df = df.withColumn(column_name, df[column_name].cast(IntegerType()))
        #
        # # uses the approxQuantile method to calculate quantiles for the specified numerical column. The quantiles
        # # will be used as bin edges for bucketizing the data.
        # quantiles = df.approxQuantile(column_name, [i / num_bins for i in range(1, num_bins)], 0.01)
        # # Ensure unique quantiles to avoid duplicates
        # quantiles = sorted(set(quantiles))
        #
        # # This line creates the final list of bin edges. It starts with negative infinity, appends the calculated
        # # quantiles, and ends with positive infinity. These edges define the ranges for the bins.
        # bin_edges = [float('-inf')] + quantiles + [float('inf')]
        # # Apply Bucketizer to the column and return the resulting Column
        # bin_column = f"{column_name}_bin"
        # bin_df = Bucketizer(splits=bin_edges, inputCol=column_name, outputCol=bin_column).transform(df)

        # print("BUCKETS")
        # bin_df.select(bin_column).show()
        # print("BIN DF", bin_df)
        #
        # # Add the new bin column to the original DataFrame
        # df = df.join(bin_df.select(column_name, bin_column), on=column_name)
        # print("RETURNING", col(column_name))
        # print("RETURNING", col(bin_column))
        # return col(column_name)
        # print("BUCKETS")
        # print(Bucketizer(splits=bin_edges, inputCol=column_name, outputCol=f"{column_name}_bin") \
        #     .transform(df).select(f"{column_name}_bin").alias(column_name))
        #
        # # Apply Bucketizer to the column and return the resulting Column
        # return Bucketizer(splits=bin_edges, inputCol=column_name, outputCol=f"{column_name}_bin") \
        #     .transform(df).select(f"{column_name}_bin").alias(column_name)

    def calculate_bins_count(self, unique_values_count):
        """
        Calculate the number of bins using Sturges' rule.
        """
        return max(int(math.log2(unique_values_count) + 1), 2)  # Sturges' formula
