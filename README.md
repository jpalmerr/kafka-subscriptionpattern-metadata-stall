## Run

From this folder:

```
bash reproduce.sh
```

### Before running

- Docker is running (`docker info`).
- Java 17 or later (`java -version`). The script exits if it finds an older one.
- Network access to Docker Hub (`apache/kafka:4.3.1`) and Maven Central (kafka-clients and slf4j jars).

### What to expect

- A run takes about 4 minutes.
- About 75 s in, it prints which consumers threw: `regex-4.1.0`, `regex-4.2.0` and `regex-4.3.1`, each with the exception.
- After the 120 s observation, it prints the exceptions again, a verdict table, and a final line. On success the final line starts with `REPRODUCED:`, with `regex-4.0.0` and `names-4.1.0` OK and the three 4.1+ consumers STALLED.
- Logs go to `$TMPDIR/kafka-regex-stall/out/<timestamp>-.../` (the script prints the path). Jars are cached in `$TMPDIR/kafka-regex-stall/jars`, so later runs are faster. Nothing is written into this folder.
- Ctrl-C removes the containers and the Docker network. If any are left behind, the next run removes them before it starts.
