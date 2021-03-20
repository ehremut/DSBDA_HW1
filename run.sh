echo "Start script ..."

rm input/*
wait

python3 gen.py
wait 

mv input_file input/

wait

echo "Remove input and output from hdfs ..."

hdfs dfs -rm -r output
wait

hdfs dfs -rm -r input
wait

echo "Inputing data"
hdfs dfs -put input input
wait

echo "Start work ..."

yarn jar /root/.m2/repository/bdtc/lab1/1.0-SNAPSHOT/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
wait

echo "Please wait, i neeed time ...."

hadoop fs -text output/part-r-00000 > decompress_file.txt
wait

echo "End script .... "
echo "Your result ..."

cat decompress_file.txt
wait
