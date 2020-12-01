# Maintainers Documentation

## Release checklist

* Please make sure CHANGELOG reflects the new version to be released
  (Usually, that's changing the `In Development` items to be reflected under
   new version and leaving `In Development` empty)
* Set the new version to be released in pom.xml and config.yml
* Please push a tag using git commands

    ```bash
    git tag -a 1.0 -m "1.0 - Shiny new features and bug fixes"
    git push upstream 1.0
    ```

* Circle CI builds the tag and ships to GitHub Releases
* `scalyr-kafka-connect-scalyr-sink-<version>.zip` is included in the GitHub Releases Downloads
