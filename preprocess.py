import csv
import json
import sys


from pyspark import SparkContext

if __name__ == "__main__":

    input_review_file = sys.argv[1]
    input_business_file = sys.argv[2]

    output_file = sys.argv[3]

    sc = SparkContext.getOrCreate()

    input_business_lines = sc.textFile(input_business_file).map(lambda lines: json.loads(lines))

    input_review_lines = sc.textFile(input_review_file).map(lambda lines: json.loads(lines))

    business_ids = input_business_lines.map(lambda kv:(kv['business_id'], kv['state'],kv['stars'])).\
                   filter(lambda kv: kv[2] >= 4)

    user_ids = input_review_lines.map(lambda kv: (kv['business_id'], kv['user_id'])).join(business_ids.map(lambda kv: (kv[0], kv[1]))). \
               map(lambda line: (line[1][0], line[1][1])).distinct().collect()


    with open(output_file, 'w', newline='') as fp:

        csv_writer = csv.writer(fp, quoting=csv.QUOTE_NONE)
        csv_writer.writerow(["user_id", "state"])
        
        for row in user_ids:
            csv_writer.writerow(row)




