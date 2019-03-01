1- Create Data File with 
create_random_video_data.py

2- put data file onto hadoop
hadoop fs -put udemy.tsv /user/hadoop/dir1/udemy.tsv

3- Get the most popular course Ids based on unique user count
select courseid,  count(distinct(userid)) user_cnt from videos group by courseid order by user_cnt desc limit 50;

4- run hive videos.hql which uses video_reducer.py to get output needed ie "[5:112,10:124,15:122,20:131]"

bin/hive -S -hiveconf PY_DIR=/Users/nest/Downloads -hiveconf HADOOP_FILE=/user/hadoop/dir1/udemy.tsv -f /Users/nest/Downloads/videos.hql
