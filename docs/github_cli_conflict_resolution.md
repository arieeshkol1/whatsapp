# Resolving Merge Conflicts with GitHub CLI

The following steps walk through resolving merge conflicts for an open pull request (for example, `Sanitize stored conversation state #109`) by using the GitHub CLI (`gh`).

1. **Check out the pull request locally**
   ```bash
   gh pr checkout 109
   ```
   This fetches the PR branch (e.g., `codex/fix-typeerror-in-_update_user_info_details-n5e7xa`) and checks it out locally so you can work on it.

2. **Sync with the latest `main` branch**
   ```bash
   git fetch origin
   git checkout main
   git pull origin main
   git checkout -
   git merge origin/main
   ```
   Pull the newest commits from `main`, then merge them into the PR branch. Resolve any conflicts that appear during the merge.

3. **Resolve conflicts in your editor**
   Open each conflicted file, look for conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`), and edit the file so it contains the desired final content. Remove the conflict markers when finished.

4. **Mark files as resolved and test**
   ```bash
   git add <file1> <file2> ...
   # run project tests or linters here
   ```
   Stage the resolved files and run the relevant test suites to ensure the merge did not break anything.

5. **Commit the resolution**
   ```bash
   git commit -m "Resolve merge conflicts with main"
   ```
   This records your conflict resolution work on the PR branch.

6. **Push the updated branch back to GitHub**
   ```bash
   git push origin HEAD
   ```
   The PR updates automatically with your resolved changes.

7. **Optionally re-run status checks**
   If the repository uses CI, trigger the workflow from the PR page or with the CLI:
   ```bash
   gh pr checks --watch 109
   ```
   Use `--watch` to stream the status until completion.

Following these steps resolves merge conflicts for the PR while keeping the branch aligned with the latest `main` changes.
