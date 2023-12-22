# сLoudER Weeker
Welcome to Clouder Weeker, the ultimate tool for electronic music enthusiasts! Our project is designed to keep you at the forefront of the ever-evolving electronic music scene. By aggregating the latest tracks across various electronic genres, we ensure you have instant access to the freshest beats out there.

## Environment Variables

The application uses the following environment variables, which are stored in a `.env` file:

- `BP_TOKEN`: This is your BP token.
- `SPOTIPY_CLIENT_ID`: This is your Spotify client ID.
- `SPOTIPY_CLIENT_SECRET`: This is your Spotify client secret.
- `SPOTIPY_REDIRECT_URI`: This is your Spotify redirect URI.
- `PG_USER`: This is your DB username.
- `PG_PASS`: This is your DB password.
- `PG_HOST`: This is your DB address.
- `PG_PORT`: This is your DB port.
- `PG_DB`: This is your DB name.
- `MONGO_USER`: This is your MongoDB username.
- `MONGO_PASS`: This is your MongoDB password.
- `MONGO_HOST`: This is your MongoDB address.
- `MONGO_PORT`: This is your MongoDB port.
- `MONGO_DB`: This is your MongoDB DB name.


## Running the Application
1. Activate the virtual environment:

2. To run the application, you must first install the dependencies:
```bash
pip install -r requirements.txt
```

3. Create a mongo database named `clouder` 

4. To create indexes in the MongoDB database, run the following command:
```bash
python raw_data.py
```

5. Then, you can run the application:
```bash
python week_collector.py
```