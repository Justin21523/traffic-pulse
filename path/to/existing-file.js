For each commit, run:
$ git add .
$ git commit -m "fix: Bug in old function"
$ git add path/to/new-feature.js path/to/docs.md
$ git commit -m "feat: Add new feature and update documentation"
$ git add .
$ git commit -m "style: Minor updates to existing file"

**Step 4: Push**
Run:
$ git remote -v
origin https://github.com/user/repo.git (fetch)
origin https://github.com/user/repo.git (push)

$ git branch --show-current
<branch-name>

$ git push -u origin <current-branch>
