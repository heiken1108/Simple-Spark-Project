from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("LetterCountDemo").setMaster("local[*]") # Initialize Spark
sc = SparkContext(conf=conf)

text_file_rdd = sc.textFile("hei.txt") # Load the text file as an RDD

letter_count = (
    text_file_rdd
    .flatMap(lambda line: list(line.lower()))  # Convert to lowercase and get all lines
    .filter(lambda char: char.isalpha())      # Filter only alphabetic characters
    .map(lambda letter: (letter, 1))           # Map each letter to a count of 1
    .reduceByKey(lambda l_1, l_2: l_1 + l_2)  # Aggregate counts of letters
    .sortBy(lambda x: x[1], ascending=False)  # Sort by count in descending order
    .collect()                                # Collect results
)

total_letters = sum(count for _, count in letter_count) # Get total number of letters

for letter, count in letter_count:
    percentage = (count / total_letters) * 100
    print(f"{letter}: {percentage:.2f}%, count: {count} of {total_letters} words")

sc.stop() #Terminate Spark


