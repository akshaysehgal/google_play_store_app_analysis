from ops import DataTransformation

def main():
    # Path to the CSV file
    csv_file_path = r"../playstore.csv"

    # Create an instance of the PySparkCSVReader class
    insights_obj = DataTransformation()

    # Execute the script
    insights_obj.read_csv(csv_file_path)

    # Stop the Spark session when you're done with all transformations
    insights_obj.spark.stop()

if __name__ == "__main__":
    # Call the main function if the script is executed
    main()