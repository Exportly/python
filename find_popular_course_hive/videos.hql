drop table if exists videos;

create table videos (userid STRING, courseId STRING, videoPosition int) ROW FORMAT DELIMITED FIELDS TERMINATED by '\t' stored as textfile;

LOAD DATA INPATH '${hiveconf:HADOOP_FILE}' OVERWRITE INTO TABLE videos;

add FILE ${hiveconf:PY_DIR}/video_reducer.py;

select TRANSFORM  (courseId, videoPosition, count(videoPosition) )  USING 'python video_reducer.py' from videos group by videoPosition, courseId;
