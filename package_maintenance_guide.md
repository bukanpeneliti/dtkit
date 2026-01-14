# Stata Package Maintenance Guide: dtkit Example

This guide summarizes best practices for versioning, tagging, and releasing updates for your Stata package (e.g., `dtkit` with components `dtfreq`, `dtstat`, `dtmeta`), primarily using Git and GitHub.

## 1. Versioning Strategy

Adopting a clear versioning strategy is crucial. We'll use **Semantic Versioning (SemVer)**: `MAJOR.MINOR.PATCH` (e.g., `1.0.0`).

* **MAJOR (`X`.y.z):** For incompatible API changes.
* **MINOR (x.`Y`.z):** For adding functionality in a backward-compatible manner.
* **PATCH (x.y.`Z`):** For backward-compatible bug fixes.

### 1.1. Overall Suite Versioning (e.g., `dtkit-vX.Y.Z`)

The `dtkit` suite has its own SemVer. This version represents a specific bundle of component versions.

* It is **NOT a sum** of component version numbers.
* It increments based on the **most significant change in any of its components** included in a `dtkit` release.
  * Bugfix in a component -> `dtkit` gets a PATCH update (e.g., `dtkit-v1.0.0` -> `dtkit-v1.0.1`).
  * New feature in a component -> `dtkit` gets a MINOR update (e.g., `dtkit-v1.0.1` -> `dtkit-v1.1.0`).
  * Breaking change in a component -> `dtkit` gets a MAJOR update (e.g., `dtkit-v1.1.0` -> `dtkit-v2.0.0`).

### 1.2. Individual Component Versioning (e.g., `dtfreq-vA.B.C`)

Each ado file (`dtfreq`, `dtstat`, `dtmeta`) also gets its own SemVer.

* Update a component's version whenever you make a meaningful change to it.
* This allows for granular tracking of changes to individual parts of your suite.

### 1.3. The `.pkg` File Version (`dtkit.pkg`)

The version specified in your `dtkit.pkg` file (e.g., `v 1.0.2 dtkit: Suite of data utilities`) should **always match the corresponding `dtkit` suite Git tag/release version**.

* Update this file *before* committing and tagging a new `dtkit` release.
* This ensures consistency, even if not immediately planning for SSC distribution.

## 2. Git Tagging

Tags mark specific points in your repository's history, primarily used for releases.

### 2.1. Annotated Tags (Recommended)

Always use **annotated tags** for releases as they store extra metadata (tagger, email, date, message).
Command: `git tag -a <tag_name> -m "Your detailed tag message"`

### 2.2. Tag Naming Conventions

* **Suite Tags:** `dtkit-vX.Y.Z` (e.g., `dtkit-v1.0.2`)
* **Component Tags:** `componentname-vA.B.C` (e.g., `dtfreq-v1.0.1`)
* Use hyphens (`-`) as separators, not underscores (`_`).
* The `v` prefix is a common convention.

### 2.3. Tag Message Content

**For Suite Tags (e.g., `dtkit-v1.0.2`):**

```text
dtkit Suite v1.0.2 - Maintenance Release

This suite release incorporates an important bug fix for the dtstat program.
Component versions included in this release:
- dtfreq: v1.0.1 (Unchanged)
- dtstat: v1.0.1 (Updated)
- dtmeta: v1.0.0 (Unchanged)

Key change:
- dtstat: Resolved error in median calculation for skewed data.

For detailed changes, please see the GitHub Release notes.
```

**For Component Tags (e.g., `dtstat-v1.0.1`):**

```text
dtstat v1.0.1 - Bugfix Release

This release addresses a specific bug in dtstat.ado:
- Resolved error in median calculation for skewed data.
  (Implemented corrected sorting and selection logic).

This update is specific to the dtstat program.
```

### 2.4. Pushing Tags

Tags are not pushed by default with `git push`.

* Push a specific tag: `git push origin <tag_name>` (e.g., `git push origin dtkit-v1.0.2`)
* Push all local tags not yet on remote: `git push origin --tags` (use with care)

## 3. Development Workflow for Updates (Bug Fixes, Features)

This assumes a Pull Request (PR) based workflow, which is excellent practice.

1. **Create a Branch (Local):**
    * Ensure `main` is up-to-date: `git checkout main; git pull origin main`
    * Create a descriptive branch: `git checkout -b fix/dtstat-median-bug` or `feature/dtfreq-new-option`
2. **Make Changes & Test:** Edit the relevant `.ado` files and test thoroughly.
3. **Commit Changes (Local on Branch):**
    * Stage changes: `git add component.ado`
    * Commit with a clear message: `git commit -m "Fix(dtstat): Correct median calculation logic"`
4. **Push Branch & Create Pull Request (PR):**
    * Push your fix/feature branch: `git push origin fix/dtstat-median-bug`
    * Go to GitHub and create a PR from your branch to `main`. Describe the changes.
5. **Review & Merge PR:**
    * Review the PR (even if it's your own).
    * Merge the PR into `main` via the GitHub interface.
    * Delete the remote branch via GitHub after merging.
6. **Update Local `main` Branch:**
    * `git checkout main`
    * `git pull origin main`
    Your `main` branch now contains the successfully merged changes. This is the point from which you will make a release.

## 4. Release Process (After Changes are Merged to `main`)

1. **Update `.pkg` File:**
    * Edit `dtkit.pkg`.
    * Change the `v X.Y.Z ...` line to reflect the new upcoming `dtkit` suite version.
    * Example: Change `v 1.0.1 dtkit: ...` to `v 1.0.2 dtkit: ...`
2. **Commit `.pkg` File Update:**
    * `git add dtkit.pkg`
    * `git commit -m "Docs: Update dtkit.pkg to v1.0.2 for release"`
3. **Tag Individual Component(s) (If Updated):**
    * If `dtstat.ado` was updated to what will be `v1.0.1`:

        ```bash
        git tag -a dtstat-v1.0.1 -m "dtstat v1.0.1 - Bugfix Release..."
        ```

    * (This tag points to the latest commit on `main`, which includes the `dtkit.pkg` update and the component fix).
4. **Tag the Overall Suite (`dtkit`):**
    * This tag points to the **same commit** as the component tag(s) from the previous step (the latest commit on `main`).

        ```bash
        git tag -a dtkit-v1.0.2 -m "dtkit Suite v1.0.2 - Maintenance Release..."
        ```

5. **Push Commits and Tags:**
    * Push the commit that updated `dtkit.pkg`: `git push origin main`
    * Push the relevant tags:

        ```bash
        git push origin dtstat-v1.0.1
        git push origin dtkit-v1.0.2
        # Or git push origin --tags
        ```

6. **Create GitHub Releases:**
    * Go to your GitHub repository -> Releases -> "Draft a new release".
    * Create one release for each tag you pushed (e.g., for `dtstat-v1.0.1` and for `dtkit-v1.0.2`).
    * Use the tag message as a basis for the release notes, expanding as needed.
    * Mark the `dtkit-vX.Y.Z` release as the "Latest release" if appropriate.

## 5. When to Make a Suite (`dtkit`) Release

* **You don't *have* to release `dtkit` every single time an individual ado file's internal version changes if those changes aren't meant for users yet.**
* However, for Stata packages where users typically install the whole suite, it's generally **good practice to make a new `dtkit` release when a component gets a user-facing update** (bug fix, new feature).
* **Recommended starting strategy:**
  * If you fix a bug or add a feature to an ado file and want users to have it, version that ado file, and then also create a new `dtkit` release (usually a PATCH or MINOR update to `dtkit`'s version) that includes it.
  * This ensures users who update `dtkit` always get the latest improvements.

## 6. Initial Version Reset (Private Repository Context)

If you ever need to "reset" your versioning *before anyone has seen or used your repository*:

1. **Plan:** Define your new, correct versioning scheme.
2. **Clean Local Tags:** `git tag -d <old_tag>` for all unwanted tags.
3. **Clean Remote Tags:** `git push origin --delete <old_tag>` for all unwanted tags.
4. **Correct Commits (Optional & Careful):** If necessary, use `git commit --amend` (for last commit) or `git rebase -i` (for older commits) to fix commit messages or structure. *This rewrites history.*
5. **Force Push Branch (If History Rewritten):** `git push origin main --force`. *Only safe if repo is truly private and unseen.*
6. **Create New Tags:** Apply your new, correct annotated tags.
7. **Push New Tags:** `git push origin --tags`.
8. **Set up GitHub Releases** from these new tags.

**This "hard reset" is generally NOT recommended for public/shared repositories due to its disruptive nature.**

## 7. Release Checklist (Summary)

Before tagging a new `dtkit` release:

1. [ ] All code changes for this release are complete and tested.
2. [ ] Changes are merged into the `main` branch (e.g., via PR).
3. [ ] Local `main` branch is up-to-date (`git pull origin main`).
4. [ ] `dtkit.pkg` file has been updated with the new `dtkit` version number.
5. [ ] Changes to `dtkit.pkg` have been committed.
6. [ ] (Ready to create annotated tags for changed components and the `dtkit` suite).

---
