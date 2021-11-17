Run the custom producer called ‘StreamJsonToKafka’ file using the following command in the address where the .py is:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 StreamJsonToKafka.py

By running this command, the stream.jsonl will be loaded, cleaned and then pushed to a topic called ‘cleaned_data_topic’ in a for loop to immitate the live streaming data process, This way I only load two fields that I need and throw the rest of the stream.jsonl away. Also, I only push only valid ‘ts’ and ‘uid’ to the topic which result in a much resource and time friendly manner.
The attached picture called 'Consumer.png' shows the data in the topic.

Run the custom consumer called ‘Cunsumer’ file using the following command in the address where the .py is:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 Cunsumer.py

By running this command, the data will be read from kafka. Then a schema would be applied to it and then the final aggregated data showing the count distinct of the users per timeline desired.

The attached picture called 'GrowthRateOutput.png' shows the final output which could be the answer to the business question about the growth rate of users

P.S. As I have shown in my previous assignment, we could convert the timestamp to any desired time format and analyse the data in it.
