import gzip, json
from datetime import datetime

chemin = ["data/raw/2026-05-04.jsonl.gz",  "data/raw/2026-05-05.jsonl.gz",         "data/raw/2026-05-06.jsonl.gz",  "data/raw/2026-05-07.jsonl.gz",         "data/raw/2026-05-08.jsonl.gz",  "data/raw/2026-05-09.jsonl.gz",         "data/raw/2026-05-10.jsonl.gz" ]
count = 0
polls = set()

for file_path in chemin:
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            count += 1
            polls.add(data.get('poll_ts'))

liste = sorted(polls)
dates = [datetime.fromisoformat(p) for p in liste]
for i in range(1, len(dates)):
    ecart = dates[i] - dates[i-1]
    if ecart.total_seconds() > 35:
        print(dates[i-1], dates[i], ecart)