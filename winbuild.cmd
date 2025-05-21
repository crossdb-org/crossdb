IF not exist build (mkdir build)
gcc -o build/libcrossdb.dll src/crossdb.c -fPIC -shared -lws2_32 -lpthread -lregex -static -O2 -Wl,--out-implib,build/libcrossdb.lib
gcc -o build/xdb-cli src/xdb-cli.c -lws2_32 -lpthread -lregex -static -O2
copy /Y include\crossdb.h build\
xcopy /ehiqy build\* c:\crossdb\