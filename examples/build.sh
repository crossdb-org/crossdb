[ "$1" == "" ] && echo $0 file.c&&echo $0 clean && exit 
[ "$1" == "clean" ] && rm -rf db_data && rm -rf *.bin && exit
name=`echo $1|cut -f1 -d.`
echo Build $1 -> $name.bin
gcc -o $name.bin -Wall -O2 $1 -I.. ../libcrossdb.so -lpthread -ldl