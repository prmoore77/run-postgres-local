# Run PostgreSQL locally

Run "./run_postgres_locally.sh" shell script.  It will create a detached Docker container running in the background.   
It will put data in the [data](data}) on your local machine (so data can be persisted).  Please note - the data folder will be git ignored by design.

Note: If running on MacOS - to get the greadlink command - you'll need to have [homebrew](https://brew.sh) installed, then install coreutils with:  
```brew install coreutils```

Then - you can connect to your locally running database with user: "postgres" - password: "mysecretpassword" (without quotes).
