# Piped input plugin for Embulk

This plugin reads from standard input. 

## Overview

* **Plugin type**: file input
* **Resume supported**: yes (no parallelism)

## Example

```yaml
in:
  type: pipe
  parser:
    type: csv
    columns:
      - {name: a, type: string}
      - {name: b, type: string}
```

```bash
echo "1,2" | embulk run example.yml
```

## Build

```
$ ./gradlew gem
```
