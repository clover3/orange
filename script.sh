/**
 * Created by user on 2015-12-01.
 */

#!/bin/sh

#if you want to execute in current shell, please type source ./script.sh ?????

#compile session
sbt compile

var_path=$PWD

#running session( Master or Slave)
while (true)
do
echo -e "enter the command : \c "
read  word1 word2 word3 word4 word5 word6 word7
if [ $word1 == "master" ]
then
    echo "waiting for running Master main"
    (cd $var_path/master/src/main/scala/master && sbt run $word2 )

fi

if [ $word1 == "slave" ]
then
    echo "waiting for running Slave main"
    (cd $var_path/master/src/main/scala/master && sbt run $word2 $word3 $word4 $word5 $word6 $word7 )

fi

echo "The commands you entered is: $word1 $word2 $word3 $word4 $word5 $word6 $word7"
done

                  