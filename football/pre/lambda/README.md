# AWS lamdba function for populating pre-game database
This directory should be directly synced with the AWS lambda function `arn:aws:lambda:eu-west-2:669298270618:function:automatePreGameFootball-v2`. When you make a change to this directory on __`main`__, make sure to deploy to the AWS lambda function.

## Deploying to AWS
1. Download and install [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html#install-sam-cli-instructions)
2. Run `which sam` to test installation (expected output is /usr/local/bin/sam)
3. Run `pip install awscli`
4. Run `aws configure`. Access Key: `{access_key}`, Secret Key: `{secret_key}`, region: `eu-west-2` , format: `json`
5. Download AWS Toolkit on VSCode and connect to AWS account with config file set up in step 5.
6. Right click on `automatePreGameFootball-v2` lambda function and click `Upload Lambda...`
