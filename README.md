# Recommended for Jedi

Daily MDBList recommender that builds a profile from the first 15 movies in
`mestreyodarossi/last-watched`, excludes every watched movie from that list, and
updates a public static MDBList named `Recommended for Jedi`.

## Usage

Set your MDBList API key in the environment:

```bash
export MDBLIST_API_KEY="your-key"

# Excluir Superhero e Romance
python3 recommended_for_jedi.py --exclude-genres "Superhero,Romance"

Ex.:
# Excluir gênero + keyword + ator ao mesmo tempo
python3 recommended_for_jedi.py \
  --exclude-genres "Superhero" \
  --exclude-keywords "imdb.kids-family" \
  --exclude-actors "Nicolas Cage"

# Excluir gênero + exigir nota IMDB entre 6.5 e 8.0 com pelo menos 5000 votos
export MDBLIST_API_KEY="your_api_here"
python3 recommended_for_jedi.py \
  --exclude-genres "Action,Superhero,Romance" \
  --exclude-keywords "imdb.kids-family" \
  --exclude-actors "Nicolas Cage" \
  --imdb-min 5.6 \
  --imdb-max 7.6 \
  --imdb-min-votes 10000 \
  --year-min 2016 \
  --year-max 2027

python3 server.py
Open http://localhost:8555 in your browser to access the frontend.
Press Ctrl+C to stop.

```

Run the daily update:

```bash
python3 recommended_for_jedi.py
```

Optional simulation mode:

```bash
python3 recommended_for_jedi.py --dry-run
```

## Fine-Tuning

You can refine the recommendations by excluding specific genres, keywords, or
actors, and by overriding IMDB rating/vote thresholds. **All flags are
optional** – when omitted, the script uses its built-in defaults.

```bash
# Exclude Horror and Romance movies, remove anything with "zombie" keyword,
# and block films starring Adam Sandler:
python3 recommended_for_jedi.py \
  --exclude-genres "Horror,Romance" \
  --exclude-keywords "zombie,vampire" \
  --exclude-actors "Adam Sandler,Steven Seagal"

# Require IMDB between 6.0 and 8.5 with at least 5000 votes:
python3 recommended_for_jedi.py \
  --imdb-min 6.0 \
  --imdb-max 8.5 \
  --imdb-min-votes 5000
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--exclude-genres` | CSV | *(none)* | Genres to completely exclude |
| `--exclude-keywords` | CSV | *(none)* | Keywords to completely exclude |
| `--exclude-actors` | CSV | *(none)* | Actor names to completely exclude |
| `--imdb-min` | float | 5.2 | Minimum IMDB rating |
| `--imdb-max` | float | *(no limit)* | Maximum IMDB rating |
| `--imdb-min-votes` | int | 1 000 | Minimum IMDB vote count |

## What It Does

- Reads `Last Watched` sorted by watched date ascending.
- Uses the first 15 movies to calculate the current taste profile.
- Uses all watched movies as an exclusion list.
- Also excludes any movie present in these lists:
  - `mestreyodarossi/trending-movies`
  - `mestreyodarossi/lastest-movie-releases`
  - `mestreyodarossi/combina-com-voce`
  - `mestreyodarossi/surprise-me`
  - `mestreyodarossi/fast-horror`
- Builds a candidate pool from your MDBList lists, the MDBList catalog, keyword
  pages, and search.
- Filters candidates by IMDb rating/votes and runtime.
- Scores candidates by repeated keywords, genres, Bayesian IMDb fit, year,
  runtime, age/certification, and country.
- Preserves up to 70 current list items and swaps up to 30 per daily run, except
  when watched items must be removed.
- Writes an audit report to `reports/`.
