# Code Standards

## Required Before Each Commit

- Run `go fmt ./...` before committing any changes to ensure proper code formatting. This will run go fmt on all Go files to maintain consistent style

- Run `go vet ./...` to catch any potential issues in the code. This will run go vet on all Go files to identify any suspicious constructs or potential bugs.

- Run `golangci-lint run` to check for linting issues. This will run golangci-lint on all Go files to enforce code quality and style guidelines.
