[ "$1" == "" ] && echo $0 file.c&&echo $0 clean && exit 

[ "$1" == "clean" ] && rm -rf db_data && rm -rf *.bin && rm -rf *.exe && rm -rf *.obj && rm -rf *.o && exit

[ "$CC" == "" ] && [ -e /usr/bin/clang ] && CC=clang
[ "$CC" == "" ] && CC=gcc

name=`echo $1|cut -f1 -d.`

if [ -e libcrossdb.so ]; then 
	# Linux/FreeBSD
	echo Build $1 -> $name.bin
	$CC -o $name.bin -Wall -O2 $1 -I.. ./libcrossdb.so -lpthread -ldl
elif [ -e crossdb.dll ]; then
	# Windows MINGW
	echo Build $1 -> $name.exe
	$CC -o $name.exe -Wall -O2 $1 -I.. crossdb.dll
elif [ -e libcrossdb.dylib ]; then
	# MacOS
	$CC -o x64-$name.bin -arch x86_64 -Wall -O2 $1 -I.. ./libcrossdb.dylib
	$CC -o arm64e-$name.bin -arch arm64e -Wall -O2 $1 -I.. ./libcrossdb.dylib
	lipo -create -output $name.bin x64-$name.bin arm64e-$name.bin
	rm -f x64-$name.bin arm64e-$name.bin
fi
