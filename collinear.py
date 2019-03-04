# encoding utf-8
from pyspark import SparkContext, SparkConf

# We can create a SparkConf() object and use it to initialize the spark context
conf = SparkConf().setAppName("Collinear Points").setMaster(
    "local[4]")  # Initialize spark context using 4 local cores as workers
sc = SparkContext(conf=conf)

from pyspark.rdd import RDD


def process(filename):
    """
    This is the process function used for finding collinear points using inputs from different files
    Input: Name of the test file
    Output: Set of collinear points
    """
    # Load the data file into an RDD
    rdd = sc.textFile(filename)

    rdd = build_collinear_set(rdd)

    # Collecting the collinear points RDD in a set to remove duplicate sets of collinear points. This is for grading purposes. You may ignore this.
    res = set(rdd.collect())

    return res


def build_collinear_set(rdd):
    # YOUR CODE HERE

    rdd = find_collinear(get_cartesian(rdd.map(to_tuple)))

    # Sorting each of your returned sets of collinear points. This is for grading purposes.
    # YOU MUST NOT CHANGE THIS.
    rdd = rdd.map(to_sorted_points)

    return rdd


def find_collinear(rdd):
    # YOUR CODE HERE

    return rdd.map(find_slope).groupByKey().mapValues(list).map(format_result).filter(lambda x: len(x) > 2)


def find_slope(x):
    # YOUR CODE HERE
    print('xxxxxxxxxxxxxxxx',x)
    delt_x = x[0][0] - x[1][0]
    delt_y = x[0][1] - x[1][1]
    slope = "inf"
    if delt_x != 0:
        slope = delt_y / delt_x
    return ((x[0], slope), x[1])


# Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def get_cartesian(rdd):
    # YOUR CODE HERE
    return rdd.cartesian(rdd).filter(non_duplicates)


# Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def non_duplicates(x):
    """
    Use this function inside the get_cartesian() function to 'filter' out pairs with duplicate points
    """
    # YOUR CODE HERE
    a = x[0]
    b = x[1]
    return not (a[0] == b[0] and a[1] == b[1])


# Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def to_tuple(x):
    # YOUR CODE HERE
    return tuple(map(lambda x: int(x), x.strip().split(' ')))


def to_sorted_points(x):
    """
    Sorts and returns a tuple of points for further processing.
    """
    return tuple(sorted(x))


def format_result(x):
    x[1].append(x[0][0])
    return tuple(x[1])


if __name__ == '__main__':
    rdd = sc.textFile("/home/lisong/data-science/spark/pa1/data.txt")
    rdd = build_collinear_set(rdd)
    print(rdd.foreach(print))
