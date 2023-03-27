# Outplay

## Overview

The Outplay supports full refresh syncs

### Output schema

Several output streams are available from this source:

* [Accounts](https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24)
* [Prospects](https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24)
* [SequenceReports](https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24)
* [Sequences](https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24)
* [users](https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24)

If there are more endpoints you'd like Airbyte to support, please [create an issue.](https://github.com/airbytehq/airbyte/issues/new/choose)

### Features

| Feature | Supported? |
| :--- | :--- |
| Full Refresh Sync | Yes |
| Incremental Sync | Yes |
| SSL connection | Yes |
| Namespaces | No |

### Performance considerations

The Outplay connector should not run into Outplay API limitations under normal usage. Please [create an issue](https://github.com/airbytehq/airbyte/issues) if you see any rate limit issues that are not automatically retried successfully.

## Getting started

### Requirements

* Client ID
* Client Secret
* Start Date

### Setup guide

* `client_id`: The Client Id that can be found when creating an api integration in Outplay
* `client_secret`: The Client Secret that can be found when creating an api integration in Outplay
* `start_date`: UTC date and time in the format 2017-01-25T00:00:00Z. Any data before this date will not be replicated. Leave blank to skip this filter

## Changelog

| Version | Date | Pull Request | Subject |
| :--- | :--- | :--- | :--- |
| 0.1.0 | 2021-10-16 |  | 🎉 New Source: Outplay |

