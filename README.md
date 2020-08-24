# airdash
Visualizing PurpleAir sensor data with Plotly Dash.

## Setup

1. [Install Dokku](http://dokku.viewdocs.io/dokku/getting-started/installation/)
2. [Deploy the app](http://dokku.viewdocs.io/dokku/deployment/application-deployment/)
    * Create a new app:
    ```
    dokku apps:create app-name
    ```
    * Install Postgres plugin:
    ```
    sudo dokku plugin:install https://github.com/dokku/dokku-postgres.git
    ```
    * Create a Postgres database:
    ```
    dokku postgres:create databasename
    ```
    * Link the database and app:
    ```
    dokku postgres:link databasename app-name
    ``` 
    * Clone airdash directly to Dokku using a [plugin](https://github.com/crisward/dokku-clone).