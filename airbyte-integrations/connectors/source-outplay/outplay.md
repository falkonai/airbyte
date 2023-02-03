# Outplay

## Overview

The Outplay supports full refresh syncs

### Output schema

Several output streams are available from this source:

* [Campaigns](https://developer.salesforce.com/docs/marketing/outplay/guide/campaigns-v4.html)
* [EmailClicks](https://developer.salesforce.com/docs/marketing/outplay/guide/batch-email-clicks-v4.html)
* [ListMembership](https://developer.salesforce.com/docs/marketing/outplay/guide/list-memberships-v4.html)
* [Lists](https://developer.salesforce.com/docs/marketing/outplay/guide/lists-v4.html)
* [ProspectAccounts](https://developer.salesforce.com/docs/marketing/outplay/guide/prospect-accounts-v4.html)
* [Prospects](https://developer.salesforce.com/docs/marketing/outplay/guide/prospects-v4.html)
* [Users](https://developer.salesforce.com/docs/marketing/outplay/guide/users-v4.html)
* [VisitorActivities](https://developer.salesforce.com/docs/marketing/outplay/guide/visitor-activities-v4.html)
* [Visitors](https://developer.salesforce.com/docs/marketing/outplay/guide/visitors-v4.html)
* [Visits](https://developer.salesforce.com/docs/marketing/outplay/guide/visits-v4.html)

If there are more endpoints you'd like Airbyte to support, please [create an issue.](https://github.com/airbytehq/airbyte/issues/new/choose)

### Features

| Feature | Supported? |
| :--- | :--- |
| Full Refresh Sync | Yes |
| Incremental Sync | No |
| SSL connection | No |
| Namespaces | No |

### Performance considerations

The Outplay connector should not run into Outplay API limitations under normal usage. Please [create an issue](https://github.com/airbytehq/airbyte/issues) if you see any rate limit issues that are not automatically retried successfully.

## Getting started

### Requirements

* Outplay Account
* Outplay Business Unit ID
* Client ID
* Client Secret
* Refresh Token
* Start Date
* Is Sandbox environment?

### Setup guide

* `client_id`: The Consumer Key that can be found when viewing your app in Salesforce
* `client_secret`: The Consumer Secret that can be found when viewing your app in Salesforce
* `refresh_token`: Salesforce Refresh Token used for Airbyte to access your Salesforce account. If you don't know what this is, follow [this guide](https://medium.com/@bpmmendis94/obtain-access-refresh-tokens-from-salesforce-rest-api-a324fe4ccd9b) to retrieve it.
* `start_date`: UTC date and time in the format 2017-01-25T00:00:00Z. Any data before this date will not be replicated. Leave blank to skip this filter
* `is_sandbox`: Whether or not the the app is in a Salesforce sandbox. If you do not know what this, assume it is false.

## Changelog

| Version | Date | Pull Request | Subject |
| :--- | :--- | :--- | :--- |
| 0.1.0 | 2021-10-16 | [7091](https://github.com/airbytehq/airbyte/pull/7091) | 🎉 New Source: Outplay |

