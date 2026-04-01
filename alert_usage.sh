while true; do
    gladequota | awk '/\/glade\/derecho\/scratch\/ljaeger/ {print $2}' | sed 's/TiB//' | awk '{if ($1 >= 25) system("echo \"WARNING: Scratch usage is at "$1" TiB\" | mail -s \"GLADE scratch quota alert\" lornjaeger@proton.me")}'
    sleep 1500
done 
