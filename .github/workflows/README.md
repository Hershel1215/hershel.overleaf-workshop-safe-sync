# Github Actions Overview

We use Github Actions to automate various tasks in this repository. The following workflows are currently enabled:

- **lock-threads.yml**: Locks closed issues and pull requests after a period of inactivity.

- **vsce-package.yml**: Call `vsce package` to create a temporary VSCode extension package when a Pull Request is created/updated on the `main` branch.

- **vsce-publish.yml**: Manually publish the extension after the VS Code Marketplace and Open VSX tokens have been configured. Source forks never publish on a schedule.
