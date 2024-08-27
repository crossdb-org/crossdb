echo "################# CrossDB" 
./bench-crossdb -q -j -r 5 -n 1000
./bench-crossdb -q -j -r 5 -n 1000000

echo "################# SQLite"
./bench-sqlite -q -j -r 5 -n 1000
./bench-sqlite -q -j -r 5 -n 1000000
