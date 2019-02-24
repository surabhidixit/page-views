from datetime import datetime,timedelta
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext
import wget
import argparse


class PageViews:
    def __init__(self,
                 start_date = datetime.now(),
                 end_date =datetime.now(),
                 start_hour = datetime.now().hour,
                 end_hour = datetime.now().hour
                 ):
        self._startdate = start_date
        self._enddate = end_date
        self._starthour = start_hour
        self._endhour = end_hour
        self._conf = SparkConf().setAppName("Datadog").setMaster("local")

        self._sc = SparkContext(conf=self._conf)
        self._sqlContext = SQLContext(self._sc)
        self._spark = SparkSession.builder.master("local").appName("Datadog").getOrCreate()

    #function that iterates over each date and hour and performs the operations
    def process(self):
        current_date = self._startdate
        current_hour = self._starthour
        hour_to = 23

        while current_date <= end_date:
            if current_date == end_date:
                hour_to = end_hour
            while current_hour <= hour_to:
                input_path, output_path = self.download_data(current_date, current_hour)
                input_df, blacklist_df = self.load_data(data_file_path=input_path,
                                                      blacklist_path=os.getcwd()+"/"+"blacklist_domains_and_pages")
                self.get_top_pages(input_df,blacklist_df,output_path)
                current_hour += 1
            current_hour = 0
            current_date += timedelta(days=1)

    #function that takes the date and time, parse them and performs the download.
    def download_data(self, date, hour):

        year = ''
        month = ''
        day = ''

        if isinstance(date, datetime):

            year = str(date.year)
            month = str(date.month)
            day= str(date.day)

        elif type(date) == type(""):

            date = str(date).split(" ")[0]
            split = date.split("-")
            year = split[0]
            month = split[1]
            day = split[2]

        #prefix with 0 if single digit hour and month
        hour = str(hour)
        if len(hour) == 1:
            hour = '0'+hour

        if len(month) == 1:
            month = '0'+month

        hour = hour+'0000'

        output_filename = os.getcwd()+'/result/'+year+'/'+month+'/'+day+'/'+hour+'.csv'

        #check if we ran for this date and hour, if yes then return else continue
        if os.path.isfile(output_filename):
            return

        url = \
            'https://dumps.wikimedia.org/other/pageviews/'+ year+'/'+year+'-'+month+'/' \
            +'pageviews-'+year+month+day+'-'+hour+'.gz'
        print(url)

        local_filename = os.getcwd()+'/'+url.split('/')[-1]

        wget.download(url)
        return local_filename,output_filename

    #function that returns the dataframes for input data and blacklisted files
    def load_data(self, data_file_path=None, blacklist_path=None):

        input_rdd = self._sc.textFile(data_file_path)
        split_input_rdd = input_rdd.map(lambda x:x.split(' '))
        input_df = split_input_rdd.toDF()
        input_df = input_df.withColumnRenamed("_1","domain_code").withColumnRenamed("_2","page_title").withColumnRenamed("_3","page_views")\
                .select("domain_code", "page_title", "page_views")
        blacklist_rdd = self._sc.textFile(blacklist_path)
        split_blacklist_rdd = blacklist_rdd.map(lambda x:x.split(' '))

        blacklist_df = split_blacklist_rdd.toDF()

        blacklist_df = \
            blacklist_df.withColumnRenamed("_1","domain_code").withColumnRenamed("_2","page_title")

        return input_df,blacklist_df

    #function that filters the blacklisted pages and finds the top 25 pages by number of views
    def get_top_pages(self, input_df, blacklist_df, output_path):
        filtered = input_df.join(blacklist_df, "page_title", "left_anti")
        filtered.createOrReplaceTempView("filtered")

        top_pages = self._spark.sql("SELECT domain_code, count(page_views) as page_views "
                                    "from filtered "
                                    "group by domain_code order by page_views desc limit 25")
        top_pages.createOrReplaceTempView("filtered_ord")

        top_pages_ord = self._spark.sql("select domain_code, page_views from filtered_ord order by domain_code asc, page_views desc")

        # top_pages.write.format("com.databricks.spark.csv").option("header", "true").save(output_path)
        top_pages_ord.write.csv(path=output_path, mode="append", header="true")


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some wikipidia files.')

    parser.add_argument("-s",
                        "--startdate",
                        help="The Start Date - format YYYY-MM-DD",
                        required=True,
                        type=valid_date)

    parser.add_argument('-e', "--enddate",
                    help="The End Date format YYYY-MM-DD (Inclusive)",
                    required=True,
                    type=valid_date)

    parser.add_argument('-sh', "--start_hour",
                        help="The start hour ",
                        required=True,
                        type=int)

    parser.add_argument('-eh', "--end_hour",
                        help="The End hour ",
                        required=True,
                        type=int)

    args = parser.parse_args()
    print(" This is start date " , args.startdate)
    start_date = args.startdate
    end_date = args.enddate
    start_hour = args.start_hour
    end_hour = args.end_hour
    obj = PageViews(start_date, end_date, start_hour, end_hour)
    obj.process()