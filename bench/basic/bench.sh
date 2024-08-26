echo "################# CrossDB" >> bench.txt
./bench-crossdb -q -r 5 -n 1000 	  >> bench.txt
./bench-crossdb -q -r 5 -n 10000	  >> bench.txt
./bench-crossdb -q -r 5 -n 100000	  >> bench.txt
./bench-crossdb -q -r 5 -n 1000000	  >> bench.txt
./bench-crossdb -q -r 5 -n 10000000	  >> bench.txt
./bench-crossdb -q -r 5 -n 100000000  >> bench.txt

echo "################# SQLite" >> bench.txt
./bench-sqlite -q -r 5 -n 1000        >> bench.txt
./bench-sqlite -q -r 5 -n 10000       >> bench.txt
./bench-sqlite -q -r 5 -n 100000      >> bench.txt
./bench-sqlite -q -r 5 -n 1000000     >> bench.txt
./bench-sqlite -q -r 5 -n 10000000    >> bench.txt
./bench-sqlite -q -r 5 -n 100000000   >> bench.txt
