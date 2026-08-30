# Github Actions Overview

We use Github Actions to automate packaging and explicit releases in this repository. The following workflows are currently enabled:

- **vsce-package.yml**: Call `vsce package` to create a temporary VSCode extension package when a Pull Request is created/updated on the `main` branch.

- **vsce-publish.yml**: Manually publish the extension after the VS Code Marketplace and Open VSX tokens have been configured. Source forks never publish on a schedule.
