# How to authenticate Google Ads API with google-ads.yaml file

1. Create `google-ads.yaml` file in your home directory with the following content.

```
developer_token:
client_id:
client_secret:
refresh_token:
login_customer_id:
client_customer_id:
use_proto_plus: True
```
2. [Get Google Ads Developer Token](https://developers.google.com/google-ads/api/docs/first-call/dev-token). Add developer token id to `google-ads.yaml` file.

3. [Generate OAuth2 credentials for **desktop application**](https://developers.google.com/adwords/api/docs/guides/authentication#generate_oauth2_credentials)

* Click the download icon next to the credentials that you just created and save file to your computer.
* export path to file as environmental varible.

    `export PATH_TO_SECRETS_FILE=/path/to/secrets/file`

*  Add client_id and client_secret value to `google-ads.yaml` file.

4. Run desktop authentication with downloaded credentials file from the previous step:

```
curl -s https://raw.githubusercontent.com/googleads/google-ads-python/main/examples/authentication/generate_user_credentials.py | python3 - -c=$PATH_TO_SECRETS_FILE
```

* Copy generated refresh token and add it to `google-ads.yaml` file.

5. [Enable Google Ads API in your project](https://developers.google.com/google-ads/api/docs/first-call/oauth-cloud-project#enable_the_in_your_project).

6. Add login_customer_id and client_customer_id (MMC under which Developer token was generated) to `google-ads.yaml`. **ID should be in 11111111 format, do not add dashes as separator**.
