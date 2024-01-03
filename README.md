# decarb-etl

# env

if you need add new variable, please contact maintainer. ex: Vincent.

# Python version

pytnon 3.9

# Development

## Prerequisites

1. copy `.env.example` to `.env` and set up the variables.
2. install dependencies
   1. (depreciated) install dependencies <code>pip install -r requirements.txt</code>
   2. install [poetry](https://python-poetry.org/docs/#installation) and run <code>poetry shell</code> to up the activates the virtual environment and <code>poetry install</code>

and run the Flask Application <code>flask run</code>

## Update requirements

when you install package not exist in requirements.txt, please execute <code>poetry add {package name}</code>

## BYPASS_MAIL_SEND

when you wanna pass send mail function in local when you test, add env `BYPASS_MAIL_SEND=1`

# Swagger

url: http:{host}:5000/apidocs

# Migration

## Prerequisites(Optional)

1. Backup the Database
2. Review Migration Scripts

   - review Version like you want, use <code>flask db heads</code> to check.

## Action

1. Create a New Migration
   - add model in `app/models.py`
   - run the migration tool command <code>flask db migrate</code> to generate a new migration script.
   - use <code>flask db downgrade \<revision\></code> to rollback.
2. Apply Migration
   - run the migration tool command <code>flask db upgrade</code>
3. Verify the Migration
   - check the action is correct.
