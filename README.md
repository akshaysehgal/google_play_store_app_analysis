For this assignment, the brief description is:
1. a pyspark job to generate insights of android apps from the data.
2. count of apps based on various combination of fields provided to you.
3. Create buckets/bins for all parameters.
4. Atleast 20 unique values should be present for a numerical field to be considered for binning. ie ek numerical parameter ki binning tbhi hogi when it has more than 20 unique values.
5. Counts should be as comprehensive as possible for different combinations of properties, ie zyada se zyada combinations ya fir explain the data more in terms of binning and combination of binning.
6. You can filter out combinations which are smaller than 2% of total volume.
7. Output should be in the form of a csv file with column names and corresponding values in a name=value combination separated by
semicolon and counts in another column.
Sample output:
Price=[4-5]; genre=Art & Design; Installs=[10000-100000], 100
8. Atleast 12 columns should be chosen and you need to consider all combinations of those 12 columns. All possible subsets of a set of 12 properties will be of the order of 2^12 ~ 4096.

Eg: 
How many apps which are free and for a certain genre and launched in a particular year etc?
How many apps between certain price range, released in certain year, is adSupported and has a certain ratings and of a certain price
etc?

I was able to create buckets like
BIN RANGES
[(-inf, 10.0), (10.0, 50.0), (50.0, 100.0), (100.0, 500.0), (500.0, 1000.0), (1000.0, 5000.0), (5000.0, 10000.0), (10000.0, inf)]
+---------------------------+
|minInstalls_specific_bucket|
+---------------------------+
|             [10000.0, inf)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|           [1000.0, 5000.0)|
|            [500.0, 1000.0)|
|             [10000.0, inf)|
|           [1000.0, 5000.0)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|          [5000.0, 10000.0)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|             [10000.0, inf)|
|          [5000.0, 10000.0)|
|              [50.0, 100.0)|
|             [10000.0, inf)|
|             [10000.0, inf)|
+---------------------------+
only showing top 20 rows

But due to Spark is encountering a situation where it would typically spill data to disk (temporary storage) because it can't fit everything in memory, I was not able to geterate the complete excel,
the insights are coming correct




Steps:
1. Create a Virtual Environment and activate it
2. Install all modules from requirement.txt
3. As the code is written in a modular and scalable way, so run the main.py in the Scripts folder
