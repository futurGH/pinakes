# pinakes

a searchable index of every bluesky post you might've seen (with vector embeddings)

- `bun install`
- `bun run build`
- `./pinakes config set did did:...`
- `./pinakes backfill` (may take several hours depending on account size!)
- optionally, run `./pinakes embeddings` to generate vector embeddings
- `./pinakes search atproto --creator pfrazee.com --before 2025-01-01T00:00:00Z --parent-author pfrazee.com`

re-run backfill every once in a while to update with new posts

run `./pinakes --help` for more information

for reference, my database (2 year old account) is about 10gb without embeddings, 100gb with embeddings
