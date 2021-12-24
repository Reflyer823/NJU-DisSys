# if [ $? != 0 ];
# then
# 	echo no
# else
# 	echo yes
# fi

until [ $? != 0 ]
do
	clear
	rm log.txt
	go test -run Persist2 | tee log.txt
done
