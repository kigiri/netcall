cd a && node index &
cd b && node index
kill $(pidof "cd a && node index &")