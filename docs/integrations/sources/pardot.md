# Salesforce Pardot

Setting up the Salesforce pardot source connector involves creating a read-only Salesforce user and configuring the Salesforce connector through the Airbyte UI.

This page guides you through the process of setting up the Salesforce source connector.

## Prerequisites

* [Salesforce Account](https://login.salesforce.com/) with Enterprise access or API quota purchased
* Dedicated Salesforce [user](https://help.salesforce.com/s/articleView?id=adding_new_users.htm&type=5&language=en_US) (optional)
* (For Airbyte Open Source) Salesforce [OAuth](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_tokens_scopes.htm&type=5) credentials

## Step 1: (Optional, Recommended) Create a read-only Salesforce user

While you can set up the Salesforce connector using any Salesforce user with read permission, we recommend creating a dedicated read-only user for Airbyte. This allows you to granularly control the data Airbyte can read.

To create a dedicated read only Salesforce user:

1. [Log into Salesforce](https://login.salesforce.com/) with an admin account.
2. On the top right of the screen, click the gear icon and then click **Setup**.
3. In the left navigation bar, under Administration, click **Users** > **Profiles**. The Profiles page is displayed. Click **New profile**.
4. For Existing Profile, select **Read only**. For Profile Name, enter **Airbyte Read Only User**.
5. Click **Save**. The Profiles page is displayed. Click **Edit**.
6. Scroll down to the **Standard Object Permissions** and **Custom Object Permissions** and enable the **Read** checkbox for objects that you want to replicate via Airbyte.
7. Scroll to the top and click **Save**.
8. On the left side, under Administration, click **Users** > **Users**. The All Users page is displayed. Click **New User**.
9. Fill out the required fields:
    1. For License, select **Salesforce**.
    2. For Profile, select **Airbyte Read Only User**.
    3. For Email, make sure to use an email address that you can access.
10. Click **Save**.
11. Copy the Username and keep it accessible.
12. Log into the email you used above and verify your new Salesforce account user. You'll need to set a password as part of this process. Keep this password accessible.

## CHANGELOG

| Version | Date       | Pull Request                                             | Subject                                    |
|:--------|:-----------|:---------------------------------------------------------|:-------------------------------------------|
| `0.0.1` | 2022-09-13 | [13930](https://github.com/airbytehq/airbyte/pull/)      | Initial Creation of Pardot Connector       |
