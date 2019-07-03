Contributing to Connect File Pulse
==================================

# Found a bug ?

If you found a bug in the source code you can help us by submitting an issue or even a Pull Request to our GitHub Repository.

# Got a new idea ?

Any new feature propositions are always appreciated. Ideas are what keep this project working. The first step is to open an issue and describe your proposal so that it can be discussed. If you changes are small you can directlty proposed a Pull Request that will be reviewed before being merged.

# Coding Guidelines

 * Any code changes must include some unit tests that cover the new functionality or the bugfix.
 * Frequently rebase your works to ease the Pull Request merging.


# Commit Message Guidelines

Each commit message consists of a header, a body and a footer. The header has a special format that includes a type, a scope and a subject:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

## Type

Must be one of the following:

* **build**: Changes that affect the build system or external dependencies
* **ci**: Changes to our CI configuration files and scripts
* **docs**: Documentation only changes
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests or correcting existing tests

## Scope

Must be one of the following:

* **plugin** : A code change that affect the plugin module plugin
* **filters** : A code change that affect the filters module
* **cleanup-api** : A code change that affect the cleanup policy API
* **scel-api** : A code change that affect the Simple Connect Expression Language API
* **api** : A code change that affect the api module
* **ci** : A change to the continuous build or continuous integration
* **github-pages** : Changes to the documentation
* **checkstyle** : Changes to the checkstyle
* **examples** : Changes to the examples
* **readme** : Changes to README.md
* **changelogs** : Changes to CHANGELOG.md
* **contributing** : Changes to CONTRIBUTING.md

## Subject
The subject contains a succinct description of the change:

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize the first letter
* no dot (.) at the end

## Body
Just as in the subject, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.


## Footer

Should contain the reference to the GitHub issue that this commis **Resolves**.
