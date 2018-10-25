# Medusa Lambdas

This repository contains code for medusa lambda functions. It also
contains some other related code. For an example, see content-backup.

## content-backup

This contains code for the lambda function that will do content
backup for the main storage bucket. There will be some files that
lambda will be unable to handle because of time restrictions. So 
there is also code to handle these files on one of our servers
and to manage the necessary communications between lambda and 
that server.
