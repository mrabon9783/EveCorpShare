# GO TO URL TO ACQUIRE TOKEN (IN REDIRECT URL AFTER AUTH) REPLACE WITH YOU ID 
#https://login.eveonline.com/v2/oauth/authorize?response_type=code&redirect_uri=https://localhost/callback&state=testing&client_id={YOUR EVE APP ID}&scope=esi-wallet.read_corporation_wallets.v1%20esi-contracts.read_corporation_contracts.v1%20esi-industry.read_corporation_jobs.v1%20esi-markets.read_corporation_orders.v1


$pair = "{YOUR APP ID}:{YOUR APP SECRET}"
$bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
$encoded = [System.Convert]::ToBase64String($bytes)

curl -o token_response.json -Method Post "https://login.eveonline.com/v2/oauth/token" `
  -Headers @{ "Authorization" = "Basic $encoded"; "Content-Type" = "application/x-www-form-urlencoded" } `
  -Body "grant_type=authorization_code&code={TOKEN FROM URL REDIRECT}"

  # CREATES token_response.json - use the refresh token value in this json object as the refresh token in config.yaml