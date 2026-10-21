# PySpark Tuning Lab

An interactive, browser-based lab for comparing join strategies, shuffle partitioning, caching and adaptive execution decisions. Enter workload sizes, compare a pinned baseline, and copy a focused PySpark starting point.

This is an educational planning model. It does not run Spark jobs, connect to a cluster, or predict measured execution time. Model assumptions and primary Spark references are available inside the app.

## Publish publicly with GitHub Pages

1. Create a **public** GitHub repository named `pyspark-lab`.
2. Extract this ZIP. Upload the **contents** of the extracted `pyspark-lab` folder to your repository. The `docs` folder must be directly at the repository root, alongside this README and `package.json`.
3. Commit the files to the `main` branch.
4. Open **Settings → Pages**.
5. Set **Source** to **Deploy from a branch**.
6. Select branch **main**, folder **/docs**, and click **Save**.
7. Wait for the Pages deployment to finish, then click **Visit site**.

Your public URL will normally be `https://YOUR-USERNAME.github.io/pyspark-lab/`.

Do not upload the ZIP itself. Upload its extracted files and folders. This package contains no Sites configuration, Git history, account identifiers, or authentication credentials.

### Upload from a terminal instead

Run these commands inside the extracted `pyspark-lab` folder after creating an empty public repository. Replace `YOUR-USERNAME` first.

```bash
git init
git add .
git commit -m "Publish PySpark tuning lab"
git branch -M main
git remote add origin https://github.com/YOUR-USERNAME/pyspark-lab.git
git push -u origin main
```

Then configure Pages using **main /docs** as described above. Later pushes to `main` will update the website.

## Run locally

No build or dependency installation is needed. With Python installed:

```bash
python -m http.server 8000 --directory docs
```

Open `http://localhost:8000`. ES modules need an HTTP server; do not open `index.html` directly using a file URL.

## Validate

With Node.js installed:

```bash
npm test
```

The tests cover broadcast eligibility, outer-join build-side restrictions, memory guardrails, partition sizing, cache decisions, input validation and generated code.

## Privacy and persistence

The website is public. Each visitor's workload settings and pinned baseline stay in that visitor's browser local storage. Reset restores defaults and clears the baseline. The app has no dataset upload, analytics, server database or cluster connection. External font and documentation requests may still reach their respective providers. Browser storage does not sync between devices or site domains.

## Files

- `docs/`: ready-to-publish static website
- `tests/`: model validation
- `package.json`: test command; `private: true` prevents accidental npm publication and does not control website visibility

This is a standalone GitHub Pages export. Publishing it does not change the access settings of the original Sites deployment.
