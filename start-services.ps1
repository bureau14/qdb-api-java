$cmd = "qdb\bin\qdbd.exe"
$args = "-a 127.0.0.1:28360 --security=false --transient -r qdb/db"
$secureArgs = "-a 127.0.0.1:28361 --security=true --cluster-private-file=cluster-secret-key.txt --user-list=users.txt --transient -r qdb/securedb"

Start-Process -NoNewWindow -RedirectStandardOutput qdbd.out.txt -RedirectStandardError qdbd.err.txt $cmd $args
Start-Process -NoNewWindow -RedirectStandardOutput qdbd.secure.out.txt -RedirectStandardError qdbd.secure.err.txt $cmd $secureArgs
