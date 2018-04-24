# csye7200-team2-spring2018
Big data using Scala project - team 2

[![Build Status](https://travis-ci.org/kunalchugh91/csye7200-team2-spring2018.svg?branch=master)](https://travis-ci.org/kunalchugh91/csye7200-team2-spring2018)

This project can train CrossValidator model locally and on EMR, while prediction can be run locally.

To train model locally 
1. Program arguments: Pass 'local' to run locally.
2. Program trains on a test.csv file in the resources folder and saves the model files into resources/model folder. 

To train model on EMR
1. Program arguments: Pass 'emr <access_key> <secret_key>' to run on emr. On EMR, program expects an S3 bucket with the name 'sparkprojectbucket' containing 'test.csv' file.
2. Program saves the model files into the same s3 bucket.

To test prediction locally
1. Program loads CrossValidator model from the resources/model directory.
2. Program expects a test.csv file in the resources directory to run the prediction on.
3. Prediction is displayed to standard output.