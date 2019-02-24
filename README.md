# page-views
The code is in wikipedia.py file. To be able to run the file, we need to have Spark and Python3 installed.
Sample command to run the file: 
spark-submit wikipedia.py -s 2019-02-14 -e 2019-02-14 -sh 01 -eh 02
Once we run the application, we need to enter a date range and hour range (given by start_date, end_date, start_hour and end_hour) when prompted.
We can alternatively enter the same day in start and end dates, if we want to get the output for a single day. Similarly, we can enter the same hour in start and end hours.
The application then downloads the files for each day and hour and computes the results for each of them and saves them to the local filesystem (if it hasn’t been computed earlier). The list of blacklisted pages is already present in the project folder. 


