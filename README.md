The scripts follow those steps:
1. loading files into snowflake staging
2. create snowflake model based to losded files
3. monitor model differences and capture flagged errors
4. create data stream for those models in incremental way instead of repetitive operations
5. set up scheduled task to automate monitoring procedure
6. integrate with email and sending alerts immediately
