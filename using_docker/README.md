To run this,
1. Start the docker engine.
2. Go to the folder where the `compose.yaml` file lives.
3. Open a terminal in that file path.
4. Run the command `docker-compose up -d --build`.
5. Run Go to http://localhost:5432
6. It would open PgAdmin. Sign in with username and password in `compose.yaml`.
7. Create a new server. In the connection tab, fill the entries as specified in `compose.yaml`.
8. You'd find the database and tables there where you can interact with it.
